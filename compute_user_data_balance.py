import altair as alt
import dask.dataframe
import pandas as pd
import os

import bok.parsers
import bok.dask_infra
import bok.pd_infra
import bok.platform


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 20)


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


def compute_filtered_purchase_and_use_intermediate(outfile, client):
    """Compute an intermediate dask frame with flows from selected users and
    user purchases
    """

    all_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    ).loc[:, ["user", "bytes_up", "bytes_down", "local", "end"]]

    # Filter the balances included to users who have been minimally active
    user_active_ranges = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")
    # Drop users new to the network first active less than a week ago.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] >= 7,
    ]
    # Drop users active for less than 1 day
    users_to_analyze = users_to_analyze.loc[
        users_to_analyze["days_active"] >= 1.0,
    ]

    all_flows_length = all_flows.shape[0]
    # Filter excluded users who have not been active enough to have good data.
    user_filtered_flows = all_flows.loc[all_flows["user"].isin(users_to_analyze["user"])]
    user_filtered_flows_length = user_filtered_flows.shape[0]

    # Remove local address flows, which are zero-rated and don't give balance
    # information.
    nonlocal_user_filtered_flows = user_filtered_flows.loc[~user_filtered_flows["local"]]
    nonlocal_user_filtered_flows_length = nonlocal_user_filtered_flows.shape[0]

    # Work with our filtered flows now
    flows = nonlocal_user_filtered_flows

    # Read in the purchases data
    purchases = bok.pd_infra.read_parquet("data/clean/transactions_consolidated_TM.parquet")
    purchases = purchases.loc[
        purchases["kind"] == "purchase",
        ["timestamp", "user", "amount_bytes", "time_since_last_purchase"]
    ].rename(columns={"amount_bytes": "bytes_purchased"})

    # Filter purchases with the same users to analyze as used for flows
    purchases = purchases.loc[purchases["user"].isin(users_to_analyze["user"])]

    # Convert purchases to dask and append
    purchases["type"] = "purchase"
    purchases_df = dask.dataframe.from_pandas(purchases, npartitions=1)

    flows = flows.reset_index().rename(columns={"start": "timestamp"})
    flows["type"] = "local"
    flows["type"] = flows["type"].where(flows["local"], other="ext")
    flows = flows.drop(columns=["local"])

    aggregate = flows.append(purchases_df)

    # Apply fillna only to specific byte columns to avoid clobbering useful NaT values in timestamp columns.
    aggregate["bytes_up"] = aggregate["bytes_up"].fillna(0)
    aggregate["bytes_down"] = aggregate["bytes_down"].fillna(0)
    aggregate["bytes_purchased"] = aggregate["bytes_purchased"].fillna(0)

    # Set byte types as integers rather than floats.
    aggregate = aggregate.astype({"bytes_up": int,
                                  "bytes_down": int,
                                  "bytes_purchased": int})

    aggregate = aggregate.categorize(columns=["type"])

    print(purchases_df)
    print(flows)
    print(aggregate)

    aggregate = aggregate.set_index("timestamp").repartition(partition_size="64M",
                                                             force=True)

    write_delayed = bok.dask_infra.clean_write_parquet(aggregate, outfile, compute=False)

    results = client.compute(
        [all_flows_length, user_filtered_flows_length, nonlocal_user_filtered_flows_length, write_delayed],
        sync=True)

    print("Flows in balance intermediate stats:")
    print("Original length:{}".format(results[0]))
    print("Post user:{} ({}% selective)".format(results[1], results[1]/results[0]*100))
    print("Post user and nonlocal:{} ({}% selective)".format(results[2], results[2]/results[0]*100))


def compute_probable_zero_ranges(infile, outfile):
    """Estimate and record probable ranges for each user when their data balance is actually zero.
    """

    flows_and_purchases = bok.dask_infra.read_parquet(infile)

    user_frame = flows_and_purchases.loc[
        flows_and_purchases["user"] == "ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8"
        ]

    user_frame = user_frame.compute()

    bok.pd_infra.clean_write_parquet(user_frame, "scratch/test_zeros_frame.parquet")
    user_frame = bok.pd_infra.read_parquet("scratch/test_zeros_frame.parquet")
    print("Read in temporary frame")
    print(user_frame)

    user_frame["last_purchase_time"] = user_frame["timestamp"].where(user_frame["type"] == "purchase", other=pd.NaT)
    user_frame["last_purchase_time"] = user_frame["last_purchase_time"].fillna(value=None, method="bfill")


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

    # Resample for displaying large time periods
    user_data_spends = user_data_spends.set_index("timestamp")
    user_data_spends = user_data_spends.resample("1h").sum().reset_index()

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

    running_user_balance = user_ledger
    running_user_balance["balance"] = user_ledger["bytes"].transform(pd.Series.cumsum)

    bok.pd_infra.clean_write_parquet(running_user_balance, outpath)


def make_plot(inpath):
    gap_df = bok.pd_infra.read_parquet("data/clean/log_gaps_TM.parquet")
    running_user_balance = bok.pd_infra.read_parquet(inpath)

    # get topups separately
    topups = running_user_balance.copy()
    topups = topups.loc[topups["type"]=="topup"]
    topups = topups.reset_index()

    alt.data_transformers.disable_max_rows()

    # reset the index to recover the timestamp for plotting
    running_user_balance = running_user_balance.reset_index().sort_values("timestamp")
    print("length:", len(running_user_balance))
    print(topups.head())

    # alt.Chart(running_user_balance).mark_line(interpolate='step-after').encode(
    lines = alt.Chart(running_user_balance).mark_line(interpolate='step-after').encode(
        x="timestamp:T",
        y=alt.Y(
            "balance:Q",
            #scale=alt.Scale(domain=[0, 1000000000]),
        ),
        tooltip=["balance:Q", "timestamp:T"],
    ).properties(width=800)

    points = alt.Chart(topups).mark_point(color="#F54242").encode(
        x="timestamp:T",
        y="balance:Q",
    )

    vertline = alt.Chart(gap_df).mark_rect(color="#FFAA00").encode(
        x="start:T",
        x2="end:T",
    )

    combo = vertline + lines + points

    combo.interactive().show()


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8
    # 5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb <- small data
    test_user = "5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb"
    grouped_flows_and_purchases_file = "scratch/filtered_flows_and_purchases_TM_DIV_none_INDEX_start"
    probable_zeros_file = "scratch/probable_zero_data_balance_ranges_TM_DIV_none_INDEX_start"
    graph_temporary_file = "scratch/graphs/user_data_balance"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = bok.dask_infra.setup_dask_client()
        # reduce_to_user_pd_frame(test_user, outpath=graph_temporary_file)
        # compute_filtered_purchase_and_use_intermediate(grouped_flows_and_purchases_file, client)
        compute_probable_zero_ranges(grouped_flows_and_purchases_file, probable_zeros_file)
        client.close()
    else:
        print("Using cached results!")

    if platform.altair_support:
        print("Running altair subcommands")
        make_plot(inpath=graph_temporary_file)

    print("Exiting")
