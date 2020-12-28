import altair as alt
import dask.dataframe
import datetime
import numpy as np
import pandas as pd
import os

import infra.parsers
import infra.dask
import infra.pd
import infra.platform


def compute_user_data_purchase_histories():
    """Compute the running ledger of total data purchased by each user
    """
    # Extract data from the transactions file into a resolved pandas frame
    transactions = infra.pd.read_parquet("data/clean/transactions_DIV_none_INDEX_timestamp.parquet")

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

    all_flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start"
    ).loc[:, ["user", "bytes_up", "bytes_down", "local", "end"]]

    # Filter the balances included to users who have been minimally active
    user_active_ranges = infra.pd.read_parquet(
        "../data/clean/user_active_deltas.parquet")
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
    purchases = infra.pd.read_parquet("data/clean/transactions_consolidated_TM.parquet")
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

    write_delayed = infra.dask.clean_write_parquet(aggregate, outfile, compute=False)

    results = client.compute(
        [all_flows_length, user_filtered_flows_length, nonlocal_user_filtered_flows_length, write_delayed],
        sync=True)

    print("Flows in balance intermediate stats:")
    print("Original length:{}".format(results[0]))
    print("Post user:{} ({}% selective)".format(results[1], results[1]/results[0]*100))
    print("Post user and nonlocal:{} ({}% selective)".format(results[2], results[2]/results[0]*100))


def estimate_zero_corrected_user_balance(user_history_frame):
    """Estimate and record probable ranges for each user when their data balance is actually zero.

    Operates with Pandas rather than dask and uses lots of grouping,
    so probably is necessary. Will require significant memory for some users.
    """

    df = user_history_frame.reset_index()  # Access timestamp directly

    # Define purchases as having the same start and end time.
    df.loc[df["type"]=="purchase", "end"] = df["timestamp"]

    # Sort by end when assigning the end time (some flows start with uplink
    # before a purchase is made, which then allows the data to flow "right
    # before" the purchase)
    df = df.sort_values(["end"])

    # The end time of the last external downlink is a conservative bound on when the balance was last positive.
    df["last_ext_rx_end_time"] = df["end"].where(
        ((df["type"] == "ext") & (df["bytes_down"] > 0)),
        other=pd.NaT,
    ).fillna(value=None, method="ffill")

    # Return to a regular start time sort.
    df = df.sort_values(["timestamp"])

    possible_zero_ranges = df.loc[
        ((df["type"] == "purchase") & (df["last_ext_rx_end_time"] < df["timestamp"])),
        ["last_ext_rx_end_time", "timestamp"]
    ].rename(columns={"last_ext_rx_end_time": "range_start",
                      "timestamp": "range_end"})

    # Mark ranges as high confidence if there are overlapping failed external flows
    likely_zero_ranges = []
    for zero_range in possible_zero_ranges.itertuples():
        count = len(df.loc[
            (((df["timestamp"] >= zero_range.range_start) & (df["timestamp"] <= zero_range.range_end)) |
             ((df["end"] >= zero_range.range_start) & (df["end"] <= zero_range.range_end)) |
             ((df["timestamp"] <= zero_range.range_start) & (df["end"] >= zero_range.range_end))) &
            (df["type"] == "ext") &
            (df["bytes_down"] <= 0)
        ])
        zero_range = zero_range._asdict()
        zero_range["count"] = count
        del zero_range["Index"]
        likely_zero_ranges.append(zero_range)

    # TODO(matt9j) Cross reference against other flows to see if the backhaul is just down.
    likely_zero_ranges = pd.DataFrame(likely_zero_ranges)

    # Likely zero threshold if there are 20 or more unsuccessful flow attempts.
    if len(likely_zero_ranges) > 0:
        likely_zero_ranges = likely_zero_ranges.loc[likely_zero_ranges["count"] >= 50]

    print(len(likely_zero_ranges))

    # Compute the spend amount
    df = df.drop(columns=["last_ext_rx_end_time"])
    df["net_bytes"] = df["bytes_purchased"] - df["bytes_up"] - df["bytes_down"]

    # Append the initial offset entry
    timestamp = df["timestamp"].min() - datetime.timedelta(seconds=1)
    user = df["user"].min()
    initial_entry = {"timestamp": timestamp,
                     "user": user,
                     "bytes_up": 0,
                     "bytes_down": 0,
                     "type": "init",
                     "bytes_purchased": 0,
                     "net_bytes": pd.NA,
                     }

    # Append while ignoring the index of the appended row
    df = df.append(initial_entry, ignore_index=True)

    # Apply zeroing offsets
    tare_offsets = likely_zero_ranges
    tare_offsets = tare_offsets.assign(
        user=user,
        type="tare",
        bytes_up=0,
        bytes_down=0,
        net_bytes=0,
        bytes_purchased=0,
    )
    tare_offsets = tare_offsets.rename(columns={"range_start": "timestamp",
                                                "range_end": "end"})

    # Append while ignoring the index of the appended row
    df = df.append(tare_offsets, ignore_index=True)

    # Resort and restore index-- note, the pandas sort is not stable, so old
    # balance entries for the same second precision timestamp may be scrambled!
    df = df.set_index("timestamp").sort_values("timestamp").reset_index()

    # Compute the observed balance
    df["balance"] = df["net_bytes"].cumsum()

    # Find the initial offset (existing balance before logging to ensure never negative)
    global_minimum = df["balance"].min()
    if global_minimum < 0:
        initial_offset = -global_minimum
    else:
        initial_offset = 0

    df.loc[df["type"] == "init", ["net_bytes", "balance"]] = initial_offset

    # Recompute the observed balance after adding the offset
    df["balance"] = df["net_bytes"].cumsum()

    # Assign tare values needed to fix the balance
    # Include external flows with downlink since they may also indicate valid balance low points.
    tare_offsets = df.loc[((df["type"] == "tare") |
                           ((df["type"] == "ext") & (df["bytes_down"] > 0)))].copy()
    # Track the "reverse cumulative min" to find inter-topups where the
    # balance actually goes down. This should be the lowest balance from each
    # row forward. These are cases where we know one of the prior topups did
    # not happen at zero, or that we missed a topup entry.
    tare_offsets["cum_min_balance"] = tare_offsets.loc[::-1, ["balance"]].cummin()[::-1]

    tare_offsets = tare_offsets.loc[df["type"] == "tare"]

    # Edge case of the starting value requires the fillna, otherwise all
    # other entries get the negative of the minimum forward looking balance!
    if len(tare_offsets) > 0:
        df.loc[df["type"] == "tare", ["net_bytes"]] = \
            - tare_offsets["cum_min_balance"].diff().fillna(tare_offsets["cum_min_balance"].iloc[0])

    df = df.astype({"net_bytes": int})

    # Recompute final tared balance
    df["balance"] = df["net_bytes"].cumsum()

    # Apply last layer of haulage filtering logic to ignore accumulated negative flows.
    df.loc[df["balance"] < 0, "balance"] = 0

    # Cleanup intermediates
    if len(tare_offsets) > 0:
        df = df.drop(columns=["count"])

    return df


def _process_and_split_single_user(infile, outfile, user):
    """From dask to dask, process and zero tare a single user's history.

     Takes as input an overall dataframe file and outputs a user-specific
     dataframe file.
    """
    df = infra.dask.read_parquet(infile)

    df = df.loc[
        df["user"] == user
        ]

    df = df.compute()

    corrected_df = estimate_zero_corrected_user_balance(df)

    # The date sort is not stable, so be sure to save a unique ordering for
    # the user's balance.
    corrected_df = corrected_df.reset_index().rename(columns={"index": "user_hist_i"})
    dask_df = dask.dataframe.from_pandas(corrected_df, npartitions=1)
    dask_df = dask_df.categorize(columns=["type", "user"])
    dask_df = dask_df.repartition(partition_size="64M",
                                  force=True)

    infra.dask.clean_write_parquet(dask_df, outfile)
    print("completed tare for user:", user)


def tare_all_users(infile, out_parent_directory, client):
    users = infra.dask.read_parquet(infile)["user"].unique().compute()
    tokens = []

    batch_size = 1
    for i, user in enumerate(users):
        print("Processing and taring single user:", user)
        out_user_directory = os.path.join(out_parent_directory, user)

        compute_token = dask.delayed(_process_and_split_single_user)(infile, out_user_directory, user)
        tokens.append(compute_token)
        if (i % batch_size) == (batch_size - 1):
            print("Computing zero align for batch", i)
            client.compute(tokens, sync=True)
            tokens = []

    print("Starting dask zero cleanup computation")
    if len(tokens) > 0:
        client.compute(tokens, sync=True)
    print("Completed zero estimation augmentation")


def reduce_to_pandas(infile, outfile, client):
    print("Beginning pandas reduction")
    aggregated_balances = infra.dask.read_parquet(infile)

    # TODO For now just select a single user to debug.
    df = aggregated_balances.loc[
        aggregated_balances["user"] == "ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8"
        ]

    df = df.reset_index().set_index("user_hist_i")[["timestamp", "balance", "user", "type", "bytes_down"]]

    df = df.compute()

    infra.pd.clean_write_parquet(df, outfile)


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

    infra.pd.clean_write_parquet(running_user_balance, outpath)


def make_plot(inpath):
    gap_df = infra.pd.read_parquet("../data/clean/log_gaps_TM.parquet")
    running_user_balance = infra.pd.read_parquet(inpath)
    print(running_user_balance)
    # Limit the domain instead of the time resolution
    running_user_balance = running_user_balance.loc[(running_user_balance["timestamp"] > "2019-05-21 18:25") &
                                                    (running_user_balance["timestamp"] < "2019-05-22 23:32")]
    # print(gap_df)
    gap_df = gap_df.loc[(gap_df["start"] > "2019-05-21 18:25") &
                        (gap_df["start"] < "2019-05-22 23:32")]


    # running_user_balance = running_user_balance.loc[(running_user_balance["timestamp"] > "2019-03-19 18:25") &
    #                                                 (running_user_balance["timestamp"] < "2019-03-21 18:32")]

    # get topups separately
    topups = running_user_balance.copy()
    topups = topups.loc[
        (topups["type"]=="purchase") |
        (topups["type"] == "tare")]

    # ((topups["type"] == "ext") & (topups["bytes_down"] > 0))
    topups = topups.reset_index()

    alt.data_transformers.disable_max_rows()
    # Resample for displaying large time periods
    # running_user_balance = running_user_balance.set_index("timestamp")
    # running_user_balance = running_user_balance.resample("1h").mean()

    # reset the index to recover the timestamp for plotting
    #TODO(matt9j) Check on the sorting
    running_user_balance = running_user_balance.reset_index().sort_values("timestamp")
    print("length:", len(running_user_balance))

    # alt.Chart(running_user_balance).mark_line(interpolate='step-after').encode(
    lines = alt.Chart(running_user_balance).mark_line(interpolate='step-after').encode(
        x="timestamp:T",
        y=alt.Y(
            "balance:Q",
            #scale=alt.Scale(domain=[0, 1000000000]),
        ),
    ).properties(width=800)

    points = alt.Chart(topups).mark_point(color="#F54242", opacity=0.2, size=200).encode(
        x="timestamp:T",
        y="balance:Q",
        color="type:N",
        tooltip=["balance:Q", "timestamp:T"],
        text="timestamp",
    )

    vertline = alt.Chart(gap_df).mark_rect(color="#FFAA00").encode(
        x="start:T",
        x2="end:T",
    )

    combo = vertline + lines + points

    combo.interactive().show()


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    grouped_flows_and_purchases_file = "scratch/filtered_flows_and_purchases_TM_DIV_none_INDEX_start"
    split_tared_balance_file = "scratch/tared_user_data_balance_TM_DIV_user_INDEX_start"
    merged_balance_file = "scratch/tared_user_data_balance_TM_DIF_none_INDEX_start"
    graph_temporary_file = "scratch/graphs/user_data_balance"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = infra.dask.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        # compute_filtered_purchase_and_use_intermediate(grouped_flows_and_purchases_file, client)
        tare_all_users(grouped_flows_and_purchases_file, split_tared_balance_file, client)
        infra.dask.merge_parquet_frames(split_tared_balance_file, merged_balance_file, index_column="timestamp")
        reduce_to_pandas(merged_balance_file, graph_temporary_file, client)
        client.close()
    else:
        print("Using cached results!")

    if platform.altair_support:
        # df = bok.pd_infra.read_parquet(
        #     "scratch/tempff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8.parquet")
        # corrected_df = estimate_zero_corrected_user_balance(df)
        # corrected_df = corrected_df.reset_index().rename(columns={"index": "user_hist_i"}).drop(columns=["time_since_last_purchase"])
        # bok.pd_infra.clean_write_parquet(corrected_df, graph_temporary_file)
        print("Running altair subcommands")
        make_plot(inpath=graph_temporary_file)

    print("Exiting")
