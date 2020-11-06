""" Helpers to compute the timedeltas associated with each user in the network
"""

import datetime
import pandas as pd

import infra.constants
import infra.dask_infra
import infra.pd_infra
import infra.platform


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 20)


def reduce_flows_to_pandas(in_path, out_path):
    """Find the first and last active flow time for each user

    in_path must point to a dask dataframe with a time index and user flows
    """
    # Immediately downselect to only needed columns
    flow_frame = infra.dask_infra.read_parquet(in_path)[["user", "end"]]
    flow_frame = flow_frame.reset_index()

    # Find the first and last time the user was active
    first_active = flow_frame.groupby("user").first()[["start"]]
    last_active = flow_frame.groupby("user").last()[["end"]]

    # Merge the first and last times to one frame
    user_range_frame = first_active.merge(last_active,
                                     left_index=True,
                                     right_index=True)

    flow_frame["day_bin"] = flow_frame["start"].dt.floor("d")
    user_days = flow_frame.groupby(["user", "day_bin"]).sum()
    user_days = user_days.assign(days_online=1)
    user_days = user_days.groupby("user").sum()
    print(user_days)

    user_range_frame = user_range_frame.merge(user_days, left_index=True, right_index=True)

    user_range_frame = user_range_frame.compute()
    infra.pd_infra.clean_write_parquet(user_range_frame, out_path)


def compute_purchase_range_frame():
    """Generates a frame with the range a user has made purchases in the net
    """
    transactions = infra.pd_infra.read_parquet("data/clean/transactions_TM.parquet")

    # Each user's total amount of data purchased directly.
    purchases = transactions.loc[transactions["kind"] == "purchase"]

    # Find the first day the user made a purchase
    first_purchases = purchases.sort_values("timestamp").groupby("user").first().reset_index()[["user", "timestamp"]]
    first_purchases = first_purchases.rename(columns={"timestamp": "first_purchase"})
    last_purchases = purchases.sort_values("timestamp").groupby("user").last().reset_index()[["user", "timestamp"]]
    last_purchases = last_purchases.rename(columns={"timestamp": "last_purchase"})

    purchase_ranges = first_purchases.merge(last_purchases, on="user")

    return purchase_ranges


def _choose_earliest(row):
    if row["start"] < row["first_purchase"]:
        return row["start"], "Flow"
    else:
        return row["first_purchase"], "Purchase"


def _choose_latest(row):
    if row["end"] > row["last_purchase"]:
        return row["end"], "Flow"
    else:
        return row["last_purchase"], "Purchase"


def _count_gap_days(row, gap_day_frame):
    count = len(gap_day_frame[(gap_day_frame["day"] >= row["earliest"]) & (gap_day_frame["day"] <= row["latest"])])
    return count


def compute_user_deltas(flow_range_intermediate_file, active_delta_out_file):
    flow_ranges = infra.pd_infra.read_parquet(flow_range_intermediate_file)
    purchase_ranges = compute_purchase_range_frame()
    network_gap_days = compute_full_gap_days("data/clean/log_gaps_TM.parquet")

    # Merge on user
    flow_ranges = flow_ranges.reset_index()
    user_ranges = flow_ranges.merge(purchase_ranges, on="user")

    # Find the earliest and latest recorded points.
    user_ranges["earliest"] = user_ranges.apply(lambda row: _choose_earliest(row)[0], axis=1)
    user_ranges["earliest_kind"] = user_ranges.apply(lambda row: _choose_earliest(row)[1], axis=1)
    user_ranges["latest"] = user_ranges.apply(lambda row: _choose_latest(row)[0], axis=1)
    user_ranges["latest_kind"] = user_ranges.apply(lambda row: _choose_latest(row)[1], axis=1)

    # Actually compute the deltas
    user_ranges["delta_since_first_active"] = \
        infra.constants.MAX_DATE - user_ranges["earliest"]

    user_ranges["delta_active"] = \
        user_ranges["latest"] - user_ranges["earliest"]

    # Also store deltas as fractional days
    user_ranges["days_since_first_active"] = \
        user_ranges["delta_since_first_active"].dt.total_seconds() / float(86400)  # (seconds per day)

    user_ranges["days_active"] = \
        user_ranges["delta_active"].dt.total_seconds() / float(86400)  # (seconds per day)

    # Count impactful gap days
    user_ranges["outage_impact_days"] = user_ranges.apply(lambda row: _count_gap_days(row, network_gap_days), axis=1)
    user_ranges["optimistic_days_online"] = user_ranges["days_online"] + user_ranges["outage_impact_days"]

    # Drop un-needed intermediate columns
    user_ranges = user_ranges.drop(["start", "end", "first_purchase", "last_purchase"], axis="columns")

    infra.pd_infra.clean_write_parquet(user_ranges, active_delta_out_file)


def compute_full_gap_days(log_gaps_file):
    """Compute a dataframe with all of the days the network was completely offline"""
    gap_df = infra.pd_infra.read_parquet(log_gaps_file)
    gap_df["gap_duration"] = gap_df["end"] - gap_df["start"]

    long_gaps = gap_df.loc[gap_df["gap_duration"] > datetime.timedelta(days=1)]

    gap_days = None
    # Generate filler date ranges
    for gap in long_gaps.iloc:
        df = pd.DataFrame({"day": pd.date_range(gap.start, gap.end, freq="1D")})
        df["day"] = df["day"].dt.floor("d")
        # Drop the first entry for the partial first log day. If a gap starts
        # right at midnight, tough luck by this definition of full days -\_O_/-
        df = df.iloc[1:]

        if gap_days is None:
            gap_days = df
        else:
            gap_days = gap_days.append(df)

    gap_days = gap_days.set_index("day").sort_index().reset_index()
    return gap_days


if __name__ == "__main__":
    platform = infra.platform.read_config()

    flow_source_file = "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    temporary_file = "scratch/graphs/compute_user_active_time"
    delta_out_file = "data/clean/user_active_deltas.parquet"
    if platform.large_compute_support:
        print("Running compute tasks")
        print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
        client = infra.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_flows_to_pandas(flow_source_file, temporary_file)
        client.close()

    compute_user_deltas(temporary_file, delta_out_file)

    print("Done!")




