import altair as alt
import pandas as pd
import dask.dataframe
import dask.distributed
import os


import bok.parsers
import bok.dask_infra


def compute_user_data_purchase_histories():
    """Compute the running ledger of total data purchased by each user
    """
    # Extract data from the transactions file into a resolved pandas frame
    transactions = dask.dataframe.read_parquet("data/clean/transactions",
                                               engine="fastparquet").compute()

    # Track purchases as positive balance
    purchases = transactions.loc[
        (transactions["kind"] == "purchase")
    ].reset_index().drop(
        ["index", "amount_idr", "dest_user", "kind"], axis="columns"
    )

    purchases = purchases.set_index("timestamp").sort_values("timestamp")

    # Rename to normalize for appending with the flows log
    purchases = purchases.rename({"amount_bytes": "bytes"}, axis="columns")

    return purchases


def compute_user_data_use_history(user_id, client):
    """Compute the data use history of a single user
    """
    data_path = os.path.join(
        "data/clean/flows/typical_fqdn_DIV_user_INDEX_start", user_id)

    df = dask.dataframe.read_parquet(data_path, engine="fastparquet")

    # Convert to pandas!
    # Drop unnecessary columns to limit the memory footprint.
    df = df.drop(["user",
                  "dest_ip",
                  "user_port",
                  "dest_port",
                  "protocol",
                  "fqdn",
                  "ambiguous_fqdn_count",
                  "fqdn_source",
                  ],
                 axis="columns").compute()

    # Reset the index once it's in memory.
    df = df.reset_index().set_index("end").sort_values("end")


    # Set byte types as integers rather than floats.
    df = df.astype({"bytes_up": int, "bytes_down": int})
    df["bytes_total"] = df["bytes_up"] + df["bytes_down"]
    print("total spend df")
    print(df)

    # Get the end column back
    df = df.reset_index()

    # Rename to normalize for appending with the transaction log and remove
    # temporary columns
    df = df.rename({"end": "timestamp", "bytes_total": "bytes"}, axis="columns")

    # TODO(matt9j) Temporary
    # df = df.drop(["bytes_up", "bytes_down", "start"],
    #              axis="columns")

    return df


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    # ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8
    # 5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb <- small data
    test_user = "ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8"
    data_purchase_histories = compute_user_data_purchase_histories()
    user_data_purchases = data_purchase_histories.loc[
        data_purchase_histories["user"] == test_user
        ]

    # Normalize purchases for merging
    user_data_purchases = user_data_purchases.drop(
        ["user"], axis="columns"
    ).reset_index()

    user_data_spends = compute_user_data_use_history(test_user, client)

    # Define spends as negative
    user_data_spends["bytes"] = -user_data_spends["bytes"]

    # Set byte types as integers rather than floats.
    user_data_spends = user_data_spends.astype({"bytes": int})
    user_data_purchases = user_data_purchases.astype({"bytes": int})
    user_data_spends["type"] = "spend"
    user_data_purchases["type"] = "topup"

    # Combine spends and purchases into one frame
    user_ledger = user_data_spends.append(
        user_data_purchases
    ).set_index(
        "timestamp"
    ).sort_values(
        "timestamp"
    )



    # Convert to local time UTC+9
    user_ledger.index = user_ledger.index + pd.tseries.offsets.DateOffset(hours=9)

    # Slice an interval to view
    user_ledger = user_ledger[(user_ledger.index > '2019-05-25 00:00') & (user_ledger.index < '2019-05-26 23:59')]

    # Find gaps that could indicate shutdowns
    gap_frame = user_data_spends
    gap_frame = gap_frame.sort_values("start")
    gap_frame = gap_frame.rename({"timestamp": "end"}, axis="columns")
    gap_frame["start"] = gap_frame["start"] + pd.tseries.offsets.DateOffset(hours=9)
    gap_frame["end"] = gap_frame["end"] + pd.tseries.offsets.DateOffset(hours=9)
    gap_frame = gap_frame[(gap_frame["start"] > '2019-05-25 00:00') & (gap_frame["start"] < '2019-05-26 23:59')]
    print("Gap Frame is:")
    print(gap_frame)

    last_end_time = None
    gaps = []
    for i, entry in enumerate(gap_frame.itertuples()):
        if i % 10000 == 0:
            print("Processed flow", i)

        if last_end_time is not None:
            if entry.start > last_end_time:
                gaps.append(last_end_time)

        last_end_time = entry.end

    gap_df = pd.DataFrame(gaps, columns=["gaps"])
    print(gaps)



            # df = df.drop(["bytes_up", "bytes_down", "start"],
    #              axis="columns")


    # Resample for displaying large time periods
    #user_ledger = user_ledger.resample("1h").sum()

    running_user_balance = user_ledger
    running_user_balance["balance"] = user_ledger["bytes"].transform(pd.Series.cumsum)

    alt.data_transformers.disable_max_rows()

    # reset the index to recover the timestamp for plotting
    running_user_balance = running_user_balance.reset_index()
    print("length:", len(running_user_balance))
    print(running_user_balance)

    # alt.Chart(running_user_balance).mark_line(interpolate='step-after').encode(
    points = alt.Chart(running_user_balance).mark_circle().encode(
        x="timestamp",
        y="balance",
        color="type",
        tooltip=["balance", "timestamp"],
    ).properties(width=800)\

    vertline = alt.Chart(gap_df).mark_rule().encode(
        x="gaps"
    )

    combo = points + vertline

    combo.interactive().show()
