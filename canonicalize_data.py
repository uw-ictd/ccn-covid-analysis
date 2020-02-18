""" Loads data from raw data files and stores in an analysis friendly manner
"""

import csv
import pickle
import datetime
import pandas as pd


def remove_nuls_from_file(source_path, dest_path):
    with open(source_path, mode='rb') as sourcefile:
        with open(dest_path, mode='w+b') as destfile:
            data = sourcefile.read()
            destfile.write(data.replace(b'\x00', b''))
            print("writing")


def read_transactions_to_dataframe(transactions_file_path):
    transactions = list()
    purchases = list()
    transfers = list()
    topups = list()
    data = dict()
    users = dict()

    i = 0
    with open(transactions_file_path, newline='') as csvfile:
        transactions_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in transactions_reader:
            print(row)

            transactions.append(row)
            if transactions[i][3] not in users.keys():
                users[transactions[i][3]] = 1
            else:
                users[transactions[i][3]] += 1
            if transactions[i][2] == 'PURCHASE':
                purchases.append(row[0].split(','))
                if transactions[i][5] not in data.keys():
                    data[transactions[i][5]] = 1
                else:
                    data[transactions[i][5]] += 1
            elif transactions[i][2] == 'USERTRANSFER':
                transfers.append(row[0].split(','))
            else:
                topups.append(row[0].split(','))
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
