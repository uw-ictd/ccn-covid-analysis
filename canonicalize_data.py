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
    if ("obfuscated_a" in flow) and not ("obfuscated_b" in flow):
        # Normal case where endpoint A is the user
        if "address_a" in flow:
            # Ensure the flowlog only has one address_a, the obfuscated one!
            raise ValueError("Flowlog is malformed!")

        index = (flow["start_time"], flow["obfuscated_a"], flow["address_b"],
                 flow["port_a"], flow["port_b"], flow["transport_protocol"])

        return TypicalFlow(index_col=str(index),
                           start=flow["start_time"],
                           end=flow["end_time"],
                           user=flow["obfuscated_a"],
                           dest_ip=flow["address_b"].exploded,
                           user_port=flow["port_a"],
                           dest_port=flow["port_b"],
                           bytes_up=flow["bytes_a_to_b"],
                           bytes_down=flow["bytes_b_to_a"],
                           protocol=flow["transport_protocol"],
                           )

    if not ("obfuscated_a" in flow) and ("obfuscated_b" in flow):
        # Normal case where endpoint B is the user
        if "address_b" in flow:
            # Ensure the flowlog only has one address_b, the obfuscated one!
            raise ValueError("Flowlog is malformed!")

        index = (flow["start_time"], flow["obfuscated_b"], flow["address_a"],
                 flow["port_b"], flow["port_a"], flow["transport_protocol"])

        return TypicalFlow(index_col=str(index),
                           start=flow["start_time"],
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
    # Tuned for matt's 16GB laptop
    dask.config.set({"dataframe.shuffle-compression": "snappy"})
    dask.config.set({"distributed.scheduler.allowed-failures": 50})
    dask.config.set({"distributed.scheduler.work-stealing": True})
    dask.config.set({"distributed.worker.memory.target": 0.2})
    dask.config.set({"distributed.worker.memory.spill": 0.4})
    dask.config.set({"distributed.worker.memory.pause": 0.6})
    dask.config.set({"distributed.worker.memory.terminate": False})
    print(dask.config.config)

    # remove_nuls_from_file("data/originals/transactions-encoded-2020-02-19.log",
    #                       "data/clean/transactions.log")
    # read_transactions_to_dataframe("data/clean/transactions.log")

    # split_lzma_file("data/originals/2019-05-17-flowlog_archive.xz",
    #                 "data/splits/2019-05-17-flowlog_archive-{:03d}.xz",
    #                 1000000)
    #
    # split_lzma_file("data/originals/2020-02-13-flowlog_archive.xz",
    #                 "data/splits/2020-02-13-flowlog_archive-{:03d}.xz",
    #                 1000000)

    # The memory limit parameter is undocumented and applies to each worker.
    cluster = dask.distributed.LocalCluster(n_workers=2,
                                            threads_per_worker=1,
                                            memory_limit='6GB')
    client = dask.distributed.Client(cluster)

    # # Convert split files to parquet
    # split_dir = os.path.join("data", "splits")
    # for filename in sorted(os.listdir(split_dir)):
    #     if not filename.endswith(".xz"):
    #         print("Skipping:", filename)
    #         continue
    #
    #     print("Converting", filename, "to parquet")
    #     working_log = import_to_dataframe(os.path.join(split_dir, filename))
    #     print("Row count ", filename, ":", len(working_log))
    #
    #     # Strip the .xz extension on output
    #     parquet_name = filename[:-3]
    #     working_log = working_log.repartition(npartitions=1)
    #     working_log.to_parquet(os.path.join(split_dir,
    #                                         "parquet",
    #                                         parquet_name),
    #                            compression="snappy")

    # Join parquet archive into one full archive
    print("Starting join...")
    aggregated_log = None

    for archive in os.listdir("data/splits/parquet"):
        filename = os.path.join("data", "splits", "parquet", archive)
        partial_log = dask.dataframe.read_parquet(filename)
        partial_log = partial_log.set_index("index_col")
        if aggregated_log is None:
            aggregated_log = partial_log
        else:
            aggregated_log = dask.dataframe.multi.concat_indexed_dataframes([aggregated_log, partial_log])
            print(aggregated_log.npartitions)
            print(aggregated_log)

            # aggregated_log = aggregated_log.merge(partial_log,
            #                                       # how="outer",
            #                                       left_index=True,
            #                                       right_index=True)

    # Repartition the final log to make it more uniform and efficient.
    aggregated_log = aggregated_log.repartition(partition_size="100MB")
    print(aggregated_log)
    print(aggregated_log.shape)
    print(len(aggregated_log))
    aggregated_log.to_parquet("scratch/big-log", compression="snappy")

    print("Starting deduplication")
    # dedup_log = dask.dataframe.read_parquet("scratch/big-log")
    # # Index on time
    # dedup_log.set_index('')
    # dedup_log = dedup_log.drop_duplicates()
    # # Repartition the final log to make it more uniform and efficient.
    # dedup_log = dedup_log.repartition(partition_size="100MB")
    # dedup_log.to_parquet("data/clean/flows", compression="snappy")

    print("import was successful!")
    client.close()
    print("Exiting hopefully cleanly...")
