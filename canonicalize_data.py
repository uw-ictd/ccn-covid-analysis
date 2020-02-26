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

from collections import Counter

from bok.datatypes import (TypicalFlow,
                           AnomalyPeerToPeerFlow,
                           AnomalyNoUserFlow)


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
        #print("TODO: Implement two user handling")
        return None

    if not ("obfuscated_a" in flow) and not ("obfuscated_b" in flow):
        # Anomaly with traffic with no user
        #print("TODO: Implement no user handling")
        return None

    raise NotImplementedError(
        "Control should not reach here, uncovered case with flow {}".format(
            str(flow)))


def import_to_dataframe(file_path):
    """Import a compressed pickle archive into dask dataframes
    """
    chunk = list()
    # Initialize an empty dask dataframe from an empty pandas dataframe. No
    # native dask empty frame constructor is available.
    aggregated_log = dask.dataframe.from_pandas(pd.DataFrame(), chunksize=1000)

    with lzma.open(file_path, mode="rb") as f:
        i = 0
        while True:
            try:
                if (i % 100000 == 0) and (len(chunk) != 0):
                    temp_frame = dask.dataframe.from_pandas(pd.DataFrame(chunk), chunksize=100000)
                    aggregated_log = aggregated_log.append(temp_frame)
                    del chunk
                    chunk = list()
                    print("Processed", i)

                flowlog = pickle.load(f)
                flow = canonicalize_flow_dict(flowlog)
                if isinstance(flow, TypicalFlow):
                    chunk.append(flow)
                else:
                    # TODO(matt9j) Aggregate additional flow types
                    pass

                i += 1
            except EOFError as e:
                temp_frame = dask.dataframe.from_pandas(pd.DataFrame(chunk), chunksize=100000)
                aggregated_log = aggregated_log.append(temp_frame)
                print("Finished processing {} with {} rows".format(
                      file_path, i))
                break

    return aggregated_log


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

    # The memory limit parameter is undocumented and applies to each worker.
    cluster = dask.distributed.LocalCluster(n_workers=3,
                                            threads_per_worker=1,
                                            memory_limit='3GB')
    client = dask.distributed.Client(cluster)

    CLEAN_TRANSACTIONS = False
    SPLIT_FLOWLOGS = False
    DEDUPLICATE_FLOWLOGS = True

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

        # Convert split files to parquet
        split_dir = os.path.join("data", "splits")
        for filename in sorted(os.listdir(split_dir)):
            if not filename.endswith(".xz"):
                print("Skipping:", filename)
                continue

            print("Converting", filename, "to parquet")
            working_log = import_to_dataframe(os.path.join(split_dir, filename))
            print("Row count ", filename, ":", len(working_log))

            # Strip the .xz extension on output
            parquet_name = filename[:-3]
            # working_log = working_log.set_index("start")
            working_log = working_log.repartition(npartitions=1)
            working_log.to_parquet(os.path.join(split_dir,
                                                "parquet",
                                                parquet_name),
                                   compression="snappy",
                                   engine="pyarrow")

    if DEDUPLICATE_FLOWLOGS:
        # Join *all* parquet split archives into one full archive
        print("Starting de-duplication join...")
        logs_to_aggregate = list()
        split_directory = os.path.join("data", "splits", "parquet")

        for archive in os.listdir(split_directory):
            filename = os.path.join(split_directory, archive)
            partial_log = dask.dataframe.read_parquet(filename, engine="pyarrow")
            logs_to_aggregate.append(partial_log)

        aggregated_log = dask.dataframe.multi.concat(logs_to_aggregate,
                                                     interleave_partitions=False)

        aggregated_log = aggregated_log.set_index("start")
        aggregated_log = aggregated_log.repartition(freq="4H")
        aggregated_log.to_parquet("scratch/checkpoint",
                                  compression="snappy",
                                  engine="pyarrow")

        print("-----------------------------------")
        print("Made it to deduplication checkpoint")
        print("-----------------------------------")

        aggregated_log = dask.dataframe.read_parquet("scratch/checkpoint",
                                                     engine="pyarrow")
        aggregate_length = len(aggregated_log)

        deduped_logs_to_aggregate = list()
        for i in range(aggregated_log.npartitions):
            subpart = aggregated_log.get_partition(i)
            subpart = subpart.drop_duplicates()
            deduped_logs_to_aggregate.append(subpart)

        deduped_log = dask.dataframe.multi.concat(deduped_logs_to_aggregate,
                                                  interleave_partitions=False)

        # Reset the index since partitioning may be broken by not
        # using interleaving in the concatenation above and the source
        # divisions are coming from different database
        # dumps. Interleaving results in partitions that are too large
        # to hold in memory on a laptop, and I was not able to find a
        # good way to tune the number of divisions created.
        deduped_log["start"] = deduped_log.index
        deduped_log = deduped_log.set_index("start")
        print("Index has been reset after concatenation")

        # Repartition the final log to make it more uniform and efficient.
        deduped_log = deduped_log.repartition(freq="1D")
        dedupe_length = len(deduped_log)

        print("Raw concat size:", aggregate_length)
        print("Final size:", dedupe_length)
        print("Removed {} duplicates!".format(aggregate_length - dedupe_length))
        deduped_log.to_parquet("data/clean/flows",
                               compression="snappy",
                               engine="pyarrow")

    client.close()
    print("Exiting hopefully cleanly...")
