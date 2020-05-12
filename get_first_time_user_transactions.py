"""Compute the first time a user is seen making a purchase in the network

Note this could be _before_ the trimmed data start time.
"""

import dask.dataframe

source_file = "data/clean/transactions_TZ"
destination_file = "data/clean/first_time_user_transactions.csv"

if __name__ == "__main__":
    frame = dask.dataframe.read_parquet(source_file,
                                        engine="fastparquet").compute()
    frame = frame.sort_values("timestamp")
    print(frame)

    # Open file to write to
    with open(destination_file, "w") as df:
        # Track all the users you have seen before
        users = set()

        # Write colunn headers
        df.write("start,action,user,amount,price,\n")

        purchases = frame[frame.kind == "purchase"]
        # purchases = frame

        # Write relevant data to file
        for purchase in purchases.itertuples():
            # If the line represents a purchase and this user is a new user,
            # write data to the file
            if purchase.user not in users:

                users.add(purchase.user)
                csv_string = ",".join([
                    str(purchase.timestamp),
                    str(purchase.kind),
                    str(purchase.user),
                    str(purchase.amount_bytes),
                    str(purchase.amount_idr),
                ])
                print(csv_string)
                df.write(csv_string)
