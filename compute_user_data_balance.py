import altair as alt
import pandas as pd
import os


import bok.parsers
import bok.dask_infra
import bok.pd_infra
import bok.platform


def compute_user_data_purchase_histories():
    """Compute the running ledger of total data purchased by each user
    """
    # Extract data from the transactions file into a resolved pandas frame
    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM").compute()

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
        "data/clean/flows/typical_fqdn_TM_DIV_user_INDEX_start", user_id)

    df = bok.dask_infra.read_parquet(data_path)

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


def reduce_to_user_pd_frame(user, outpath):
    data_purchase_histories = compute_user_data_purchase_histories()
    user_data_purchases = data_purchase_histories.loc[
        data_purchase_histories["user"] == user
        ]

    # Normalize purchases for merging
    user_data_purchases = user_data_purchases.drop(
        ["user"], axis="columns"
    ).reset_index()

    user_data_spends = compute_user_data_use_history(user, client)

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

    # Resample for displaying large time periods
    #user_ledger = user_ledger.resample("1h").sum()

    running_user_balance = user_ledger
    running_user_balance["balance"] = user_ledger["bytes"].transform(pd.Series.cumsum)

    bok.pd_infra.clean_write_parquet(running_user_balance, outpath)


def make_plot(inpath):
    gap_df = bok.dask_infra.read_parquet("data/clean/log_gaps_TM.parquet")
    running_user_balance = bok.pd_infra.read_parquet(inpath)

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
    ).properties(width=800)

    vertline = alt.Chart(gap_df).mark_rect().encode(
        x="start:T",
        x2="end:T",
    )

    combo = points + vertline

    combo.interactive().show()


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8
    # 5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb <- small data
    test_user = "ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8"
    graph_temporary_file = "scratch/graphs/user_data_balance"

    if platform.large_compute_support:
        client = bok.dask_infra.setup_dask_client()
        reduce_to_user_pd_frame(test_user, outpath=graph_temporary_file)

    if platform.altair_support:
        make_plot(inpath=graph_temporary_file)
