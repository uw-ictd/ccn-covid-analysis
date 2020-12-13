""" Loads data from raw data files and stores in an analysis friendly manner
"""

import mappers.domains
import infra.dask
import infra.parsers
import csv
import dask.config
import dask.dataframe
import dask.distributed
import ipaddress
import lzma
import gzip
import pandas as pd
import pickle
import os
import shutil
import socket

from collections import (Counter, defaultdict)

from infra.datatypes import (TypicalFlow,
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
            with gzip.open(out_format_pattern.format(chunk_i), mode="wb") as outfile:
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

    with gzip.open(file_path, mode="rb") as f:
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

    with gzip.open(file_path, mode="rb") as f:
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
                                                  engine="fastparquet")
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
        _clean_write_parquet(aggregated_log, "scratch/checkpoint")

        print("Wrote deduplication checkpoint!")

        aggregated_log = dask.dataframe.read_parquet("scratch/checkpoint",
                                                     engine="fastparquet")

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
                                           engine="fastparquet",
                                           compute=False)
    results = client.compute([aggregate_length, dedupe_length, write_delayed],
                             sync=True)

    print("Raw concat size:", results[0])
    print("Final size:", results[1])
    print("Removed {} duplicates!".format(results[0] - results[1]))


def _clean_write_parquet(dataframe, path):
    return infra.dask.clean_write_parquet(dataframe, path)


def split_by_user(flowlog_path, dns_path, client):
    """Annotate flows with the fqdn they are likely communicating with
    """
    print("Generate minimized DNS frame")
    slim_dns_frame = dask.dataframe.read_parquet(
        "../scratch/dns/aggregated/typical",
        engine="fastparquet")
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

    deduped_logs_to_aggregate = list()
    for i in range(slim_dns_frame.npartitions):
        subpart = slim_dns_frame.get_partition(i)
        subpart = subpart.drop_duplicates(subset=["user",
                                                  "domain_name",
                                                  "ip_address"])
        deduped_logs_to_aggregate.append(subpart)

    slim_dns_frame = dask.dataframe.multi.concat(deduped_logs_to_aggregate,
                                                 interleave_partitions=False)

    print("slim_length =", len(slim_dns_frame))
    print("slim partitions=", slim_dns_frame.npartitions)

    _clean_write_parquet(slim_dns_frame, "../scratch/dns/slim")

    slim_dns_frame = dask.dataframe.read_parquet("../scratch/dns/slim",
                                                 engine="fastparquet", )

    # Partition by user.

    print("setting index")
    slim_dns_frame = slim_dns_frame.reset_index()
    slim_dns_frame = slim_dns_frame.set_index("user").repartition(npartitions=200)
    _clean_write_parquet(slim_dns_frame, "../scratch/dns/slim_user_indexed")

    slim_dns_frame = dask.dataframe.read_parquet(
        "../scratch/dns/slim_user_indexed",
        engine="fastparquet", )

    dns_by_users = slim_dns_frame.groupby("user")
    users_in_dns_log = slim_dns_frame.index.unique()
    print(users_in_dns_log.values)
    for user in users_in_dns_log:
        print("running user", user)
        user_dns_logs = dns_by_users.get_group(user)
        _clean_write_parquet(user_dns_logs,
                             "scratch/dns/slim_per_user/" + str(user))

    print("chopping flows")
    flows = dask.dataframe.read_parquet("../scratch/flows/aggregated/typical",
                                        engine="fastparquet")

    flows = flows.reset_index()
    flows = flows.set_index("user")
    _clean_write_parquet(flows, "../scratch/flows/aggregated_indexed_by_user")

    flows = dask.dataframe.read_parquet(
        "../scratch/flows/aggregated_indexed_by_user", engine="fastparquet")
    print("read!")
    flows = flows.repartition(npartitions=200)
    print("repartitioned!")

    _clean_write_parquet(flows,
                         "../scratch/flows/aggregated_indexed_by_user_partitioned")
    print("wrote")
    flows = dask.dataframe.read_parquet(
        "../scratch/flows/aggregated_indexed_by_user_partitioned",
        engine="fastparquet")
    flows_by_user = flows.groupby("user")

    users_in_flow_log = flows.index.unique()
    print(users_in_flow_log.values)
    for user in users_in_flow_log:
        print("Flow for user:", user)
        user_flow_logs = flows_by_user.get_group(user)
        _clean_write_parquet(user_flow_logs,
                             "scratch/flows/per_user/" + str(user))

    print("running sorts")
    users_in_dns_log = sorted(os.listdir("../scratch/dns/slim_per_user/"))
    users_in_flow_log = sorted(os.listdir("../scratch/flows/per_user"))

    for user in users_in_dns_log:
        print("DNS user:", user)
        frame = dask.dataframe.read_parquet("scratch/dns/slim_per_user/" + str(user),
                                            engine="fastparquet")
        frame = frame.reset_index().set_index("timestamp").repartition(partition_size="64M",
                                                                       force=True)
        _clean_write_parquet(frame, "scratch/dns/successful_DIV_user_INDEX_timestamp/" + str(user))

    for user in users_in_flow_log:
        print("Flow user:", user)
        frame = dask.dataframe.read_parquet("scratch/flows/per_user/" + str(user),
                                            engine="fastparquet")
        frame = frame.reset_index().set_index("start").repartition(partition_size="64M",
                                                                   force=True)
        _clean_write_parquet(frame, "scratch/flows/typical_DIV_user_INDEX_start/" + str(user))


def augment_user_flow_with_dns(flow_frame,
                               dns_frame,
                               reverse_dns_cache,
                               reverse_dns_failures):
    """Iterate over the flows and track DNS state for the user

    Will not be correct unless the flow and dns are indexed by time
    """

    # Bookeeping for building a new frame
    max_rows_per_division = 10000
    out_chunk = list()
    # Initialize an empty dask dataframe from an empty pandas dataframe. No
    # native dask empty frame constructor is available.
    out_frame = dask.dataframe.from_pandas(pd.DataFrame(),
                                           chunksize=max_rows_per_division)

    dns_state = dict()
    dns_ambiguity_state = defaultdict(set)
    dns_iterator = dns_frame.itertuples()
    pending_dns_log = None

    for i, flow in enumerate(flow_frame.itertuples()):
        if i % 10000 == 0:
            print("Processed flow", i)

        flow_start_time = flow.Index

        if (pending_dns_log is not None and
            pending_dns_log.Index < flow_start_time):
            # Handle any dangling DNS logs from previous flow loop iterations
            dns_state[pending_dns_log.ip_address] = pending_dns_log.domain_name
            dns_ambiguity_state[pending_dns_log.ip_address].add(
                pending_dns_log.domain_name)
            pending_dns_log = None

        # Advance DNS to the first log past the start of the flow
        while ((dns_iterator is not None) and
               (pending_dns_log is None)):
            try:
                dns_log = next(dns_iterator)
                if dns_log.Index < flow_start_time:
                    # Account for the dns log immediately.
                    dns_state[dns_log.ip_address] = dns_log.domain_name
                    dns_ambiguity_state[dns_log.ip_address].add(dns_log.domain_name)
                else:
                    # DNS has now advanced up to or beyond the flow frontier,
                    # store this log for future comparisons.
                    pending_dns_log = dns_log

            except StopIteration:
                # We made it to the end of the DNS log! On the off chance the
                # dask iterators are not well behaved, stop calling next on them
                # after they stop once : )
                dns_iterator = None

        # Update the flow fqdn if available!
        augmented_flow = flow._asdict()
        if flow.dest_ip in dns_state:
            # The most accurate result is the domain the user was actually
            # trying to reach.
            augmented_flow["fqdn"] = dns_state[flow.dest_ip]
            augmented_flow["ambiguous_fqdn_count"] = len(dns_ambiguity_state[flow.dest_ip])
            augmented_flow["fqdn_source"] = "user_dns_log"
        else:
            # Attempt to lookup the name if needed and the address is global
            # and likely to be observable from the US.
            if ((flow.dest_ip not in reverse_dns_cache) and
                (flow.dest_ip not in reverse_dns_failures) and
                    ipaddress.ip_address(flow.dest_ip).is_global):
                try:
                    lookup = socket.gethostbyaddr(flow.dest_ip)
                    reverse_dns_cache[flow.dest_ip] = lookup[0]
                    print("Looked up:", flow.dest_ip, "-->", lookup[0])
                except socket.herror:
                    # Unable to find a domain name!
                    print("Failed lookup:", flow.dest_ip)
                    reverse_dns_failures.add(flow.dest_ip)
                    pass

            # Fill from rDNS if available
            if flow.dest_ip in reverse_dns_cache:
                augmented_flow["fqdn"] = reverse_dns_cache[flow.dest_ip]
                augmented_flow["ambiguous_fqdn_count"] = 1
                augmented_flow["fqdn_source"] = "reverse_dns"
            else:
                augmented_flow["fqdn"] = ""
                augmented_flow["ambiguous_fqdn_count"] = 0
                augmented_flow["fqdn_source"] = "none"

        out_chunk.append(augmented_flow)
        if len(out_chunk) >= max_rows_per_division:
            out_frame = out_frame.append(
                dask.dataframe.from_pandas(
                    pd.DataFrame(out_chunk),
                    chunksize=max_rows_per_division,
                )
            )
            out_chunk = list()

    if len(out_chunk) > 0:
        out_frame = out_frame.append(dask.dataframe.from_pandas(
            pd.DataFrame(out_chunk),
            chunksize=max_rows_per_division,
            )
        )
    out_frame = out_frame.rename(columns={"Index": "start"})

    out_frame = out_frame.set_index("start", sorted=True).repartition(partition_size="64M",
                                                                      force=True)
    out_frame = out_frame.categorize(columns=["fqdn_source"])
    return out_frame


def categorize_fqdn_from_parquet(in_path, out_path, compute=True):
    """Run categorization over the input parquet file and write to the output

    Requires the input parquet file specify an `fqdn` column
    """
    frame = dask.dataframe.read_parquet(
        in_path,
        engine="fastparquet")

    frame["category"] = frame.apply(
        lambda row: mappers.domains.assign_category(row["fqdn"]),
        axis="columns",
        meta=("category", object))

    return _clean_write_parquet(
        frame,
        out_path,
        compute=compute)


if __name__ == "__main__":
    client = infra.dask.setup_dask_client()

    CLEAN_TRANSACTIONS = False

    SPLIT_DNS_LOGS = False
    SPLIT_FLOWLOGS = False

    INGEST_FLOWLOGS = False
    DEDUPLICATE_FLOWLOGS = False

    INGEST_DNSLOGS = False
    DEDUPLICATE_DNSLOGS = False

    BUILD_PER_USER_INDEXES = False

    COMBINE_DNS_WITH_FLOWS = False
    CATEGORIZE_USER_FLOWS = False

    RE_MERGE_FLOWS = False

    if CLEAN_TRANSACTIONS:
        remove_nuls_from_file(
            "../data/original-raw-archives/transactions-encoded-2020-05-04.log",
            "data/transactions.log")

        transactions = infra.parsers.parse_transactions_log(
            "../data/transactions.log")
        transactions = dask.dataframe.from_pandas(transactions, chunksize=100000)

        infra.dask.clean_write_parquet(transactions, "../data/clean/transactions")

    if SPLIT_DNS_LOGS:
        split_lzma_file("data/original-raw-archives/2020-11-16-dns_archive.xz",
                        "scratch/splits/dns/archives/2020-11-16-dns_archive-{:03d}.gz",
                        1000000)

    if SPLIT_FLOWLOGS:
        split_lzma_file("data/originals/2019-05-17-flowlog_archive.xz",
                        "scratch/splits/flows/archives/2019-05-17-flowlog_archive-{:03d}.gz",
                        1000000)

        split_lzma_file("data/originals/2020-02-13-flowlog_archive.xz",
                        "scratch/splits/flows/archives/2020-02-13-flowlog_archive-{:03d}.gz",
                        1000000)

        split_lzma_file("data/originals/2020-05-04-flowlog_archive.xz",
                        "scratch/splits/flows/archives/2020-05-04-flowlog_archive-{:03d}.gz",
                        1000000)

        split_lzma_file("data/original-raw-archives/2020-11-16-flowlog_archive.xz",
                        "scratch/splits/flows/archives/2020-11-16-flowlog_archive-{:03d}.gz",
                        1000000)

    if INGEST_FLOWLOGS:
        # Import split files and archive to parquet
        split_dir = os.path.join("scratch", "splits", "flows")
        archive_dir = os.path.join(split_dir, "archives")
        for filename in sorted(os.listdir(archive_dir)):
            if not filename.endswith(".gz"):
                print("Skipping:", filename)
                continue

            print("Converting", filename, "to parquet")
            frames = import_flowlog_to_dataframes(os.path.join(archive_dir, filename))
            for index, working_log in enumerate(frames):
                if len(working_log) == 0:
                    continue

                print("Row count ", filename, ":", index, ":", len(working_log))
                # Strip the .xz extension on output
                parquet_name = filename[:-3]

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

                _clean_write_parquet(working_log, out_path)

    if DEDUPLICATE_FLOWLOGS:
        input_path = os.path.join("scratch", "splits", "flows", "parquet")
        output_path = os.path.join("scratch", "flows", "aggregated")
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
                                 index_column="start",
                                 time_slice="4H",
                                 checkpoint=True,
                                 client=client)

    if INGEST_DNSLOGS:
        # Import dns logs and archive to parquet
        dns_archives_directory = os.path.join("scratch", "splits", "dns", "archives")
        for filename in sorted(os.listdir(dns_archives_directory)):
            if not filename.endswith(".gz"):
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

                dns_type = None
                if index == 0:
                    dns_type = "typical"
                else:
                    raise RuntimeError("PANICCCSSSSS")

                out_path = os.path.join(dns_archives_directory,
                                        "parquet",
                                        dns_type,
                                        parquet_name)

                _clean_write_parquet(working_log, out_path)

    if DEDUPLICATE_DNSLOGS:
        input_path = os.path.join("scratch", "splits", "dns", "parquet")
        output_path = os.path.join("scratch", "dns", "aggregated")
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

    if BUILD_PER_USER_INDEXES:
        # Eventually maybe parameterize this better, for now it's a hack
        split_by_user(None, None, None)

    if COMBINE_DNS_WITH_FLOWS:
        dns_cache_path = "../scratch/reverse_dns_cache.pickle"
        try:
            with open(dns_cache_path, mode="rb") as f:
                reverse_dns_cache = pickle.load(f)
        except FileNotFoundError:
            # Start the cache fresh
            reverse_dns_cache = dict()

        dns_fail_cache_path = "../scratch/reverse_dns_failures.pickle"
        try:
            with open(dns_fail_cache_path, mode="rb") as f:
                dns_fail_cache = pickle.load(f)
        except FileNotFoundError:
            # Start the cache fresh
            dns_fail_cache = set()

        users_in_dns_log = sorted(os.listdir("scratch/dns/successful_DIV_user_INDEX_timestamp/"))
        users_in_flow_log = sorted(os.listdir("scratch/flows/typical_DIV_user_INDEX_start/"))
        missing_dns_users = list()
        for user in users_in_flow_log:
            if user not in users_in_dns_log:
                print("Missing dns for user with flows:", user)
                missing_dns_users.append(user)
                continue

            print("Doing dns to flow mapping for user:", user)
            flow_frame = dask.dataframe.read_parquet(
                "scratch/flows/typical_DIV_user_INDEX_start/" + str(user),
                engine="fastparquet")

            dns_frame = dask.dataframe.read_parquet(
                "scratch/dns/successful_DIV_user_INDEX_timestamp/" + str(user),
                engine="fastparquet")

            augmented_flow_frame = augment_user_flow_with_dns(flow_frame,
                                                              dns_frame,
                                                              reverse_dns_cache,
                                                              dns_fail_cache)
            print(augmented_flow_frame)
            print(augmented_flow_frame.head(10, compute=True))
            _clean_write_parquet(
                augmented_flow_frame,
                "scratch/flows/typical_fqdn_DIV_user_INDEX_start/" + str(user))

            print("Saving reverse dns cache to:", dns_cache_path)
            with open(dns_cache_path, mode="w+b") as f:
                pickle.dump(reverse_dns_cache, f)

            print("Saving reverse dns failures cache to:", dns_fail_cache_path)
            with open(dns_fail_cache_path, mode="w+b") as f:
                pickle.dump(dns_fail_cache, f)

        print("Completed DNS augmentation")
        print("The following users had no DNS logs")
        print(missing_dns_users)

    if CATEGORIZE_USER_FLOWS:
        print("Categorizing user flows")
        users = sorted(os.listdir("scratch/flows/typical_fqdn_DIV_user_INDEX_start"))

        computation_futures = []
        for user in users:
            print("Categorizing flows for:", user)
            in_path = "scratch/flows/typical_fqdn_DIV_user_INDEX_start/" + str(user)
            out_path = "scratch/flows/typical_fqdn_category_DIV_user_INDEX_start/" + str(user)
            handle = categorize_fqdn_from_parquet(in_path, out_path, compute=False)
            computation_futures.append(handle)

        print("Realizing futures")
        results = client.compute(computation_futures, sync=True)

    if RE_MERGE_FLOWS:
        merged_frame = None

        users_on_disk = sorted(os.listdir("scratch/flows/typical_fqdn_category_DIV_user_INDEX_start/"))
        for user in users_on_disk:
            flow_frame = dask.dataframe.read_parquet(
                "scratch/flows/typical_fqdn_category_DIV_user_INDEX_start/" + str(user),
                engine="fastparquet")

            if merged_frame is None:
                merged_frame = flow_frame
            else:
                merged_frame = merged_frame.append(flow_frame)

        merged_frame = merged_frame.reset_index().set_index(
            "start"
        ).repartition(
            partition_size="64M",
            force=True
        )

        print("writing")
        _clean_write_parquet(merged_frame, "scratch/flows/typical_fqdn_category_DIV_none_INDEX_start/")

    client.close()
    print("Exiting hopefully cleanly...")
