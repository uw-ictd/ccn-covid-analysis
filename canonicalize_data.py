""" Loads data from raw data files and stores in an analysis friendly manner
"""

import csv
import pickle
import datetime
import pandas as pd

from collections import Counter


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

    frame = pd.DataFrame({'cost in indonesian rupiah': list(data.keys()),
                          'transactions': list(data.values())})
    frame2 = pd.DataFrame({'user': list(users.keys()),
                           'transactions': list(users.values())})

    frame.to_hdf("transactions_data.h5", key="cost_counts", mode="w")
    frame2.to_hdf("transaction_data.h5", key="user_counts", mode="a")


if __name__ == "__main__":
    remove_nuls_from_file("data/transactions-2020-02-13-coded.log",
                          "data/transactions-2020-02-13-coded-recovered.log")
    read_transactions_to_dataframe("data/transactions-2020-02-13-coded-recovered.log")
