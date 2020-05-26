import altair as alt
import dask.dataframe
import datetime
import numpy as np
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
pd.set_option('display.max_rows', 40)


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

    # flows_and_purchases = bok.dask_infra.read_parquet(infile)
    #
    # df = flows_and_purchases.loc[
    #     flows_and_purchases["user"] == "ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8"
    #     ]
    #
    # df = df.compute()
    #
    # bok.pd_infra.clean_write_parquet(df, "scratch/test_zeros_frame.parquet")
    df = bok.pd_infra.read_parquet("scratch/test_zeros_frame.parquet")

    df = df.reset_index()  # Access timestamp directly

    # Purchases mark a fencepost where we know the data balance should go positive.
    df["next_purchase_time"] = df["timestamp"].where(
        df["type"] == "purchase",
        other=pd.NaT,
    ).fillna(value=None, method="bfill")

    # The end time of the last external downlink is a conservative bound on when the balance was last positive.
    df["last_ext_rx_end_time"] = df["end"].where(
        ((df["type"] == "ext") & (df["bytes_down"] > 0)),
        other=pd.NaT,
    ).fillna(value=None, method="ffill")

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
        likely_zero_ranges.append(zero_range)

    # TODO(matt9j) Cross reference against other flows to see if the backhaul is just down.
    likely_zero_ranges = pd.DataFrame(likely_zero_ranges)

    # Likely zero threshold if there are 20 or more unsuccessful flow attempts.
    likely_zero_ranges = likely_zero_ranges.loc[likely_zero_ranges["count"] >= 20]

    # Compute the spend amount
    df = df.drop(columns=["last_ext_rx_end_time", "next_purchase_time"])
    df["net_bytes"] = df["bytes_purchased"] - df["bytes_up"] - df["bytes_down"]

    # Append the initial offset entry
    timestamp = df["timestamp"].min() - datetime.timedelta(seconds=1)
    user = df["user"].min()
    initial_entry = {"timestamp": timestamp,
                     "user": user,
                     "bytes_up": 0,
                     "bytes_down": 0,
                     "type": "init",
                     "bytes_purchased": pd.NA,  # We will fill these values later
                     "net_bytes": pd.NA,
                     }

    # Append while ignoring the index of the appended row
    df = df.append(initial_entry, ignore_index=True)

    # Apply zeroing offsets
    print(likely_zero_ranges)
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
    # Include external flows with downlink since they may also indicate balance low points.
    tare_offsets = df.loc[((df["type"] == "tare") |
                           ((df["type"] == "ext") & (df["bytes_down"] > 0)))].copy()
    # Track the "reverse cumulative min" to find inter-topups where the
    # balance actually goes down. This should be the lowest balance from each
    # row forward. These are cases where we know one of the prior topups did
    # not happen at zero, or that we missed a topup entry.
    print(df)
    tare_offsets["cum_min_balance"] = tare_offsets.loc[::-1, ["balance"]].cummin()[::-1]
    print(tare_offsets)

    tare_offsets = tare_offsets.loc[df["type"] == "tare"]

    # Edge case of the starting value requires the fillna, otherwise all
    # other entries get the negative of the minimum forward looking balance!
    df.loc[df["type"] == "tare", ["net_bytes"]] = \
        - tare_offsets["cum_min_balance"].diff().fillna(tare_offsets["cum_min_balance"].iloc[0])

    df = df.astype({"net_bytes": int})

    # Recompute final tared balance
    df["balance"] = df["net_bytes"].cumsum()

    print(df)
    print(df.loc[df["type"] == "tare"])
    print(df.loc[((df["type"] == "purchase") & (df["timestamp"] >= "2019-03-16 09:00:00"))])
    print(df.loc[df["balance"] < 0])
    return df


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
        # TODO (Remove)
        compute_probable_zero_ranges(grouped_flows_and_purchases_file, probable_zeros_file)

    if platform.altair_support:
        print("Running altair subcommands")
        make_plot(inpath=graph_temporary_file)

    print("Exiting")
