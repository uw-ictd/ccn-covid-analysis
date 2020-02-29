""" Loads data from raw data files and stores in an analysis friendly manner
"""

import csv
import dask.config
import dask.dataframe
import dask.distributed
import lzma
import pandas as pd
import pickle
import os
import shutil

from collections import Counter

from bok.datatypes import (TypicalFlow,
                           AnomalyPeerToPeerFlow,
                           AnomalyNoUserFlow,
                           DnsResponse)


def remove_nuls_from_file(source_path, dest_path):
    with open(source_path, mode='rb') as sourcefile:
        data = sourcefile.read()
        nul_count = data.count(b'\x00')
        if nul_count > 0:
            print("{} nul values found-- file is corrupted".format(nul_count))
            _print_nuls_with_context(data)

        # Remove nuls from the file.
        with open(dest_path, mode='w+b') as destfile:
            destfile.write(data.replace(b'\x00', b''))


def _print_nuls_with_context(binary_stream):
    """Print the context around runs of nul values"""
    start_run_index = None
    for index, byte in enumerate(binary_stream):
        if byte == 0x00:
            if start_run_index is None:
                start_run_index = index
        else:
            if start_run_index is not None:
                nul_count = index - start_run_index
                print("nul from [{}, {})".format(start_run_index, index))
                print(repr(binary_stream[start_run_index-30:start_run_index]) +
                      "{}X".format(nul_count) + '*NUL* ' +
                      repr(binary_stream[index:index+30]))
                start_run_index = None


def read_transactions_to_dataframe(transactions_file_path):
    transactions = list()
    purchases = list()
    transfers = list()
    topups = list()
    data = Counter()
    users = Counter()

    i = 0
    with open(transactions_file_path, newline='') as csvfile:
        transactions_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in transactions_reader:
            transactions.append(row)
            users[transactions[i][3]] += 1

            if transactions[i][2] == 'PURCHASE':
                purchases.append(row)
                data[transactions[i][5]] += 1
            elif transactions[i][2] == 'USERTRANSFER':
                transfers.append(row)
            else:
                topups.append(row)
            i += 1

    frame = dask.DataFrame({'cost in indonesian rupiah': list(data.keys()),
                          'transactions': list(data.values())})
    frame2 = dask.DataFrame({'user': list(users.keys()),
                           'transactions': list(users.values())})

    frame.to_hdf("transactions_data.h5", key="cost_counts", mode="w")
    frame2.to_hdf("transaction_data.h5", key="user_counts", mode="a")

    return transactions, purchases, transfers, topups, data, users


def split_lzma_file(input_path, out_format_pattern, chunksize):
    """Split LZMA files on disk into chunks of [chunksize] pickled objects

    This is more sophisticated than just running `split` on the unix command
    line since it is pickled object aware, splits on a number of objects
    rather than a number of bytes, and ensures splits happen on whole object
    boundaries.
    """
    with lzma.open(input_path, mode="rb") as infile:
        row_i = 0
        chunk_i = 0
        should_continue = True
        while should_continue:
            with lzma.open(out_format_pattern.format(chunk_i), mode="wb") as outfile:
                for i in range(chunksize):
                    try:
                        flowlog = pickle.load(infile)
                        pickle.dump(flowlog, outfile)
                    except EOFError as e:
                        print("finished processing at row ", row_i)
                        should_continue = False
                        break

                    row_i += 1
            print("Finished exporting row", row_i, "into chunk", chunk_i)
            chunk_i += 1


def canonicalize_flow_dict(flow):
    if ("obfuscated_a" in flow) and ("obfuscated_b" not in flow):
        # Normal case where endpoint A is the user
        if "address_a" in flow:
            # Ensure the flowlog only has one address_a, the obfuscated one!
            raise ValueError("Flowlog is malformed!")

        return TypicalFlow(start=flow["start_time"],
                           end=flow["end_time"],
                           user=flow["obfuscated_a"],
                           dest_ip=flow["address_b"].exploded,
                           user_port=flow["port_a"],
                           dest_port=flow["port_b"],
                           bytes_up=flow["bytes_a_to_b"],
                           bytes_down=flow["bytes_b_to_a"],
                           protocol=flow["transport_protocol"],
                           )

    if ("obfuscated_a" not in flow) and ("obfuscated_b" in flow):
        # Normal case where endpoint B is the user
        if "address_b" in flow:
            # Ensure the flowlog only has one address_b, the obfuscated one!
            raise ValueError("Flowlog is malformed!")

        return TypicalFlow(start=flow["start_time"],
                           end=flow["end_time"],
                           user=flow["obfuscated_b"],
                           dest_ip=flow["address_a"].exploded,
                           user_port=flow["port_b"],
                           dest_port=flow["port_a"],
                           bytes_up=flow["bytes_b_to_a"],
                           bytes_down=flow["bytes_a_to_b"],
                           protocol=flow["transport_protocol"],
                           )

    if ("obfuscated_a" in flow) and ("obfuscated_b" in flow):
        # Anomaly with user-user LAN traffic
        if ("address_a" in flow) or ("address_b" in flow):
            # Ensure the flowlog only has obfuscated addresses!
            raise ValueError("Flowlog is malformed!")

        return AnomalyPeerToPeerFlow(start=flow["start_time"],
                                     end=flow["end_time"],
                                     user_a=flow["obfuscated_a"],
                                     user_b=flow["obfuscated_b"],
                                     a_port=flow["port_a"],
                                     b_port=flow["port_b"],
                                     bytes_a_to_b=flow["bytes_a_to_b"],
                                     bytes_b_to_a=flow["bytes_b_to_a"],
                                     protocol=flow["transport_protocol"],
                                     )

    if ("obfuscated_a" not in flow) and ("obfuscated_b" not in flow):
        # Anomaly with traffic with no user
        if ("address_a" not in flow) or ("address_b" not in flow):
            # The flowlog is missing an address!
            raise ValueError("Flowlog is malformed!")

        return AnomalyNoUserFlow(start=flow["start_time"],
                                 end=flow["end_time"],
                                 ip_a=flow["address_a"].exploded,
                                 ip_b=flow["address_b"].exploded,
                                 a_port=flow["port_a"],
                                 b_port=flow["port_b"],
                                 bytes_a_to_b=flow["bytes_a_to_b"],
                                 bytes_b_to_a=flow["bytes_b_to_a"],
                                 protocol=flow["transport_protocol"],
                                 )

    raise NotImplementedError(
        "Control should not reach here, uncovered case with flow {}".format(
            str(flow)))


def canonicalize_dnslog_dict(dns_response):
    """Takes a raw DNS log and normalizes the address direction and naming
    """
    logs_to_return = list()
    if ("obfuscated_src" in dns_response) and ("obfuscated_dst" not in dns_response):
        if "src_ip" in dns_response:
            raise ValueError("Malformed dns log {}".format(dns_response))
        if "dest_ip" not in dns_response:
            return [dns_response]

        for address, ttl in zip(dns_response["response_addresses"],
                                dns_response["response_ttls"]):
            logs_to_return.append(
                DnsResponse(timestamp=dns_response["timestamp"],
                            user=dns_response["obfuscated_src"],
                            dns_server=dns_response["dest_ip"].exploded,
                            user_port=dns_response["src_port"],
                            server_port=dns_response["dst_port"],
                            protocol=dns_response["protocol"],
                            opcode=dns_response["opcode"],
                            resultcode=dns_response["resultcode"],
                            domain_name=dns_response["host"],
                            ip_address=address.exploded,
                            ttl=ttl,
                            )
            )

        return logs_to_return

    if ("obfuscated_src" not in dns_response) and ("obfuscated_dst" in dns_response):
        if "dst_ip" in dns_response:
            raise ValueError("Malformed dns log {}".format(dns_response))
        if "src_ip" not in dns_response:
            return [dns_response]

        for address, ttl in zip(dns_response["response_addresses"],
                                dns_response["response_ttls"]):
            logs_to_return.append(
                DnsResponse(timestamp=dns_response["timestamp"],
                            user=dns_response["obfuscated_dst"],
                            dns_server=dns_response["src_ip"].exploded,
                            user_port=dns_response["dst_port"],
                            server_port=dns_response["src_port"],
                            protocol=dns_response["protocol"],
                            opcode=dns_response["opcode"],
                            resultcode=dns_response["resultcode"],
                            domain_name=dns_response["host"],
                            ip_address=address.exploded,
                            ttl=ttl,
                            )
            )

        return logs_to_return

    return [dns_response]
    raise NotImplementedError("Unsupported DNS log: {}".format(
        str(dns_response)))


def import_flowlog_to_dataframes(file_path):
    """Import a compressed pickle archive into dask dataframes

    Returns 3 dataframes:
    0: all standard user->site flows
    1: All user->user flows
    2: All ip->ip flows
    """
    max_rows_per_division = 10000
    chunks = [list(), list(), list()]
    # Initialize an empty dask dataframe from an empty pandas dataframe. No
    # native dask empty frame constructor is available.
    frames = [dask.dataframe.from_pandas(pd.DataFrame(), chunksize=max_rows_per_division),
              dask.dataframe.from_pandas(pd.DataFrame(), chunksize=max_rows_per_division),
              dask.dataframe.from_pandas(pd.DataFrame(), chunksize=max_rows_per_division),
              ]

    with lzma.open(file_path, mode="rb") as f:
        i = 0
        while True:
            try:
                # Log loop progress
                if i % 100000 == 0:
                    print("Processed", i)

                # Load data
                flowlog = pickle.load(f)
                flow = canonicalize_flow_dict(flowlog)
                if isinstance(flow, TypicalFlow):
                    chunks[0].append(flow)
                elif isinstance(flow, AnomalyPeerToPeerFlow):
                    chunks[1].append(flow)
                elif isinstance(flow, AnomalyNoUserFlow):
                    chunks[2].append(flow)
                else:
                    print(flow)
                    raise ValueError("Flow type was unable to be parsed")

                # Create a new division if needed.
                for index, chunk in enumerate(chunks):
                    if len(chunk) >= max_rows_per_division:
                        frames[index] = frames[index].append(
                            dask.dataframe.from_pandas(
                                pd.DataFrame(chunk),
                                chunksize=max_rows_per_division,
                            )
                        )
                        chunks[index] = list()

            except EOFError as e:
                # An exception at the end of the file is accepted and normal
                break

            i += 1

    # Clean up and add any remaining entries.
    for index, chunk in enumerate(chunks):
        if len(chunk) > 0:
            frames[index] = frames[index].append(
                dask.dataframe.from_pandas(
                    pd.DataFrame(chunk),
                    chunksize=max_rows_per_division,
                )
            )

    print("Finished processing {} with {} rows".format(
          file_path, i))

    return frames


def import_dnslog_to_dataframes(file_path):
    """Import a compressed pickle archive of DNS queries into dask dataframes

    Returns 1 dataframe:
    0: all standard user->dns->user responses
    """
    max_rows_per_division = 10000
    chunks = [list()]
    # Initialize an empty dask dataframe from an empty pandas dataframe. No
    # native dask empty frame constructor is available.
    frames = [dask.dataframe.from_pandas(pd.DataFrame(), chunksize=max_rows_per_division),
              ]

    with lzma.open(file_path, mode="rb") as f:
        i = 0
        response_count = 0
        while True:
            try:
                # Log loop progress
                if i % 100000 == 0:
                    print("Processed", i)

                # Load data
                dnslog = pickle.load(f)
                dns_responses = canonicalize_dnslog_dict(dnslog)
                for response in dns_responses:
                    if isinstance(response, DnsResponse):
                        chunks[0].append(response)
                        response_count += 1
                    else:
                        print("----------------")
                        print("--Bad DNS type--")
                        print(response)
                        # raise ValueError("DNS type was unable to be parsed")

                # Create a new division if needed.
                for index, chunk in enumerate(chunks):
                    if len(chunk) >= max_rows_per_division:
                        frames[index] = frames[index].append(
                            dask.dataframe.from_pandas(
                                pd.DataFrame(chunk),
                                chunksize=max_rows_per_division,
                            )
                        )
                        chunks[index] = list()

            except EOFError as e:
                # An exception at the end of the file is accepted and normal
                break

            i += 1

    # Clean up and add any remaining entries.
    for index, chunk in enumerate(chunks):
        if len(chunk) > 0:
            frames[index] = frames[index].append(
                dask.dataframe.from_pandas(
                    pd.DataFrame(chunk),
                    chunksize=max_rows_per_division,
                )
            )

    print("Finished processing {} with {} rows and {} responses".format(
        file_path, i, response_count))

    return frames


def consolidate_datasets(input_directory,
                         output,
                         index_column,
                         time_slice,
                         checkpoint=False,
                         client=None):
    """Load all data from input, concatenate into one deduplicated output
    """
    logs_to_aggregate = list()

    for archive in os.listdir(input_directory):
        archive_path = os.path.join(input_directory, archive)
        partial_log = dask.dataframe.read_parquet(archive_path,
                                                  engine="pyarrow")
        logs_to_aggregate.append(partial_log)

    aggregated_log = dask.dataframe.multi.concat(logs_to_aggregate,
                                                 interleave_partitions=False)

    # Reset the index since partitioning may be broken by not
    # using interleaving in the concatenation above and the source
    # divisions are coming from different database
    # dumps. Interleaving results in partitions that are too large
    # to hold in memory on a laptop, and I was not able to find a
    # good way to tune the number of divisions created.
    aggregated_log = aggregated_log.reset_index()
    aggregated_log = aggregated_log.set_index(index_column)

    # This repartition must be done on one of the keys we wish to check
    # uniqueness against below!
    aggregated_log = aggregated_log.repartition(freq=time_slice, force=True)

    if checkpoint:
        try:
            shutil.rmtree("scratch/checkpoint")
        except FileNotFoundError:
            # No worries if the output doesn't exist yet.
            pass

        aggregated_log.to_parquet("scratch/checkpoint",
                                  compression="snappy",
                                  engine="pyarrow")

        print("Wrote deduplication checkpoint!")

        aggregated_log = dask.dataframe.read_parquet("scratch/checkpoint",
                                                     engine="pyarrow")

    aggregate_length = aggregated_log.shape[0]

    # Run deduplicate on the log subparts binned by date.
    # This only works since timestamp is part of the uniqueness criteria!
    deduped_logs_to_aggregate = list()
    for i in range(aggregated_log.npartitions):
        subpart = aggregated_log.get_partition(i)
        subpart = subpart.drop_duplicates()
        deduped_logs_to_aggregate.append(subpart)

    deduped_log = dask.dataframe.multi.concat(deduped_logs_to_aggregate,
                                              interleave_partitions=False)
    dedupe_length = deduped_log.shape[0]

    write_delayed = deduped_log.to_parquet(output,
                                           compression="snappy",
                                           engine="pyarrow",
                                           compute=False)
    results = client.compute([aggregate_length, dedupe_length, write_delayed],
                             sync=True)

    print("Raw concat size:", results[0])
    print("Final size:", results[1])
    print("Removed {} duplicates!".format(results[0] - results[1]))


def drop_and_reindex(input_path, output_path, index_key=None, drops=None, reset_index=False):
    frame = dask.dataframe.read_parquet(input_path, engine="pyarrow")
    print("Dropping and re-indexing {} to {}".format(input_path, output_path))
    print("{} has {} partitions".format(input_path, frame.npartitions))
    if drops is not None:
        frame = frame.drop(drops, axis="columns")

    if reset_index:
        frame = frame.reset_index()

    if index_key is not None:
        frame = frame.set_index(index_key)

    try:
        shutil.rmtree(output_path)
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass

    #print("{} has {} partitions".format(output_path, frame.npartitions))
    frame.to_parquet(output_path,
                     compression="snappy",
                     engine="pyarrow",
                     compute=True)


def annotate_flows_with_dns(flowlog_path, dns_path, client):
    """Annotate flows with the fqdn they are likely communicating with
    """
    def _combine_user_addr(row):
        return row['user'] + row['dest_ip']

    print("Generate minimized DNS frame")
    slim_dns_frame = dask.dataframe.read_parquet("data/clean/dnslogs/typical",
                                                 engine="pyarrow")
    print("Slim dns has {} partitions".format(slim_dns_frame.npartitions))
    slim_dns_frame = slim_dns_frame.drop(["dns_server",
                                          "user_port",
                                          "server_port",
                                          "protocol",
                                          "opcode",
                                          "resultcode",
                                          "ttl",
                                          ],
                                          axis="columns")

    # Ensure the timestamp doesn't get lost in the merge.
    slim_dns_frame = slim_dns_frame.reset_index()

    deduped_logs_to_aggregate = list()
    for i in range(slim_dns_frame.npartitions):
        subpart = slim_dns_frame.get_partition(i)
        subpart = subpart.drop_duplicates(subset=["user",
                                                  "domain_name",
                                                  "ip_address"])
        deduped_logs_to_aggregate.append(subpart)

    slim_dns_frame = dask.dataframe.multi.concat(deduped_logs_to_aggregate,
                                                 interleave_partitions=False)

    # Change column names to overlap for merge
    slim_dns_frame = slim_dns_frame.rename(columns={"ip_address": "dest_ip"})

    slim_dns_frame["user_and_addr"] = slim_dns_frame.apply(_combine_user_addr, axis=1)

    slim_dns_frame = slim_dns_frame.set_index("user_and_addr", npartitions=1000)

    print("slim_length =", len(slim_dns_frame))
    print("slim partitions=", slim_dns_frame.npartitions)

    try:
        shutil.rmtree("scratch/dnslogs/slim")
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass
    print(slim_dns_frame)
    slim_dns_frame.to_parquet("scratch/dnslogs/slim",
                              compression="snappy",
                              engine="pyarrow",
                              compute=True)

    print("chopping flows")
    flows = dask.dataframe.read_parquet("data/clean/flows/typical",
                                        engine="pyarrow")
    flows = flows.reset_index()

    flows["user_and_addr"] = flows.apply(_combine_user_addr, axis=1)

    flows = flows.set_index("user_and_addr", npartitions=2000)

    try:
        shutil.rmtree("scratch/flows/slim")
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass
    flows.to_parquet("scratch/flows/slim",
                     compression="snappy",
                     engine="pyarrow",
                     compute=True)

    print("Loading realized intermediate indexed datasets")
    flows = dask.dataframe.read_parquet("scratch/flows/slim",
                                        engine="pyarrow")
    slim_dns_frame = dask.dataframe.read_parquet("scratch/dnslogs/slim",
                                                 engine="pyarrow")

    print("Partition counts")
    print("flows", flows.npartitions)
    print("dns", slim_dns_frame.npartitions)

    merged_flows = flows.merge(slim_dns_frame,
                               how="left",
                               left_index=True,
                               right_index=True,
                               indicator=True)

    print("merged partitions:", merged_flows.npartitions)
    print(merged_flows)

    try:
        shutil.rmtree("scratch/flows/dns_merged")
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass
    merged_flows.to_parquet("scratch/flows/dns_merged",
                            compression="snappy",
                            engine="pyarrow",
                            compute=True,
                            partition_on=["user"])


if __name__ == "__main__":
    # ------------------------------------------------
    # Dask tuning, currently set for a 16GB RAM laptop
    # ------------------------------------------------

    # Compression sounds nice, but results in spikes on decompression
    # that can lead to unstable RAM use and overflow.
    dask.config.set({"dataframe.shuffle-compression": False})
    dask.config.set({"distributed.scheduler.allowed-failures": 50})
    dask.config.set({"distributed.scheduler.work-stealing": True})

    # Aggressively write to disk but don't kill worker processes if
    # they stray. With a small number of workers each worker killed is
    # big loss. The OOM killer will take care of the overall system.
    dask.config.set({"distributed.worker.memory.target": 0.2})
    dask.config.set({"distributed.worker.memory.spill": 0.4})
    dask.config.set({"distributed.worker.memory.pause": 0.6})
    dask.config.set({"distributed.worker.memory.terminate": False})

    # Remove memory warning logs
    dask.config.set({"logging.distributed": "error"})
    dask.config.set({"logging.'distributed.worker'": "error"})

    # Shuffle with disk
    dask.config.set(shuffle='disk')

    # The memory limit parameter is undocumented and applies to each worker.
    cluster = dask.distributed.LocalCluster(n_workers=3,
                                            threads_per_worker=1,
                                            memory_limit='3GB')
    client = dask.distributed.Client(cluster)

    CLEAN_TRANSACTIONS = False
    SPLIT_FLOWLOGS = False
    INGEST_FLOWLOGS = False
    DEDUPLICATE_FLOWLOGS = False

    INGEST_DNSLOGS = False
    DEDUPLICATE_DNSLOGS = False

    COMBINE_DNS_WITH_FLOWS = True

    if CLEAN_TRANSACTIONS:
        remove_nuls_from_file("data/originals/transactions-encoded-2020-02-19.log",
                              "data/clean/transactions.log")
        read_transactions_to_dataframe("data/clean/transactions.log")

    if SPLIT_FLOWLOGS:
        split_lzma_file("data/originals/2019-05-17-flowlog_archive.xz",
                        "data/splits/2019-05-17-flowlog_archive-{:03d}.xz",
                        1000000)

        split_lzma_file("data/originals/2020-02-13-flowlog_archive.xz",
                        "data/splits/2020-02-13-flowlog_archive-{:03d}.xz",
                        1000000)

    if INGEST_FLOWLOGS:
        # Import split files and archive to parquet
        split_dir = os.path.join("scratch", "splits")
        for filename in sorted(os.listdir(split_dir)):
            if not filename.endswith(".xz"):
                print("Skipping:", filename)
                continue

            print("Converting", filename, "to parquet")
            frames = import_flowlog_to_dataframes(os.path.join(split_dir, filename))
            for index, working_log in enumerate(frames):
                if len(working_log) == 0:
                    continue

                print("Row count ", filename, ":", index, ":", len(working_log))
                # Strip the .xz extension on output
                parquet_name = filename[:-3]
                working_log = working_log.set_index("start")
                working_log = working_log.repartition(partition_size="32M",
                                                      force=True)

                flow_type = None
                if index == 0:
                    flow_type = "typical"
                elif index == 1:
                    flow_type = "p2p"
                elif index == 2:
                    flow_type = "nouser"

                out_path = os.path.join(split_dir,
                                        "parquet",
                                        flow_type,
                                        parquet_name)
                try:
                    shutil.rmtree(out_path)
                except FileNotFoundError:
                    # No worries if the output doesn't exist yet.
                    pass

                working_log.to_parquet(out_path,
                                       compression="snappy",
                                       engine="pyarrow")

    if DEDUPLICATE_FLOWLOGS:
        input_path = os.path.join("scratch", "splits", "parquet")
        output_path = os.path.join("scratch", "test_output")
        for case_kind in ["typical", "p2p", "nouser"]:
            specific_output = os.path.join(output_path, case_kind)
            try:
                shutil.rmtree(specific_output)
            except FileNotFoundError:
                # No worries if the output doesn't exist yet.
                pass

            print("Starting de-duplication join for {} flows...".format(
                case_kind))
            consolidate_datasets(input_directory=os.path.join(input_path,
                                                              case_kind),
                                 output=specific_output,
                                 index_column="start")

    if INGEST_DNSLOGS:
        # Import dns logs and archive to parquet
        dns_archives_directory = os.path.join("scratch", "dns", "splits")
        for filename in sorted(os.listdir(dns_archives_directory)):
            if not filename.endswith(".xz"):
                print("Skipping:", filename)
                continue

            print("Converting", filename, "to parquet")
            frames = import_dnslog_to_dataframes(os.path.join(dns_archives_directory, filename))
            for index, working_log in enumerate(frames):
                if len(working_log) == 0:
                    continue

                print("Row count ", filename, ":", index, ":", len(working_log))
                # Strip the .xz extension on output
                parquet_name = filename[:-3]
                working_log = working_log.set_index("timestamp")
                working_log = working_log.repartition(partition_size="32M",
                                                      force=True)
                dns_type = None
                if index == 0:
                    dns_type = "typical"
                else:
                    raise RuntimeError("PANICCCSSSSS")

                out_path = os.path.join(dns_archives_directory,
                                        "parquet",
                                        dns_type,
                                        parquet_name)
                try:
                    shutil.rmtree(out_path)
                except FileNotFoundError:
                    # No worries if the output doesn't exist yet.
                    pass

                working_log.to_parquet(out_path,
                                       compression="snappy",
                                       engine="pyarrow")

    if DEDUPLICATE_DNSLOGS:
        input_path = os.path.join("scratch", "dns", "splits", "parquet")
        output_path = os.path.join("scratch", "dns", "deduplicated_output")
        for case_kind in ["typical"]:
            specific_output = os.path.join(output_path, case_kind)
            try:
                shutil.rmtree(specific_output)
            except FileNotFoundError:
                # No worries if the output doesn't exist yet.
                pass

            print("Starting de-duplication join for {} dns...".format(
                case_kind))
            consolidate_datasets(input_directory=os.path.join(input_path,
                                                              case_kind),
                                 output=specific_output,
                                 index_column="timestamp",
                                 time_slice="4H",
                                 checkpoint=True,
                                 client=client)

    if COMBINE_DNS_WITH_FLOWS:
        annotate_flows_with_dns(None, None, None)

    client.close()
    print("Exiting hopefully cleanly...")
