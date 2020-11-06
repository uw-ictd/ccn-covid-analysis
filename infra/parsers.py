import csv
import dateutil
import pandas as pd

from infra.datatypes import Transaction


# Read to a structured dataframe
def parse_transactions_log(log_path):
    """Parse the transactions log into a pandas dataframe
    """
    transactions = list()
    with open(log_path, newline='') as csvfile:
        transreader = csv.reader(csvfile, delimiter = ' ', quotechar = '|')
        for row in transreader:
            timestamp = dateutil.parser.parse(" ".join([row[0], row[1]]))
            if row[2] == 'PURCHASE':
                item = Transaction(timestamp=timestamp,
                                   kind="purchase",
                                   user=row[3],
                                   dest_user=None,
                                   amount_bytes=int(row[4]),
                                   amount_idr=int(row[5]),
                                   )
                transactions.append(item)
            elif row[2] == 'USERTRANSFER':
                amount = row[5]
                if amount == "":
                    # Record null values as 0
                    amount = "0"
                if "." in amount:
                    # Round floats to the nearest IDR
                    amount = round(float(amount))

                item = Transaction(timestamp=timestamp,
                                   kind="user_transfer",
                                   user=row[3],
                                   dest_user=row[4],
                                   amount_bytes=None,
                                   amount_idr=int(amount),
                                   )
                transactions.append(item)
            elif row[2] == 'TOPUP':
                item = Transaction(timestamp=timestamp,
                                   kind="admin_topup",
                                   user=None,
                                   dest_user=row[3],
                                   amount_bytes=None,
                                   amount_idr=int(row[4]),
                                   )
                transactions.append(item)
            elif row[2] == "ADMINTRANSFER":
                item = Transaction(timestamp=timestamp,
                                   kind="admin_transfer",
                                   user=row[3],
                                   dest_user=row[4],
                                   amount_bytes=None,
                                   amount_idr=int(row[5]),
                                   )
                transactions.append(item)
            else:
                raise ValueError("Unknown transaction type: [{}]".format(row))

    transactions = pd.DataFrame(transactions)
    transactions["kind"] = transactions["kind"].astype("category")

    return transactions
