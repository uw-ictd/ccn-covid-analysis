""" Helpers to compute the timedeltas associated with each user in the network
"""

import datetime
import pandas as pd

import bok.constants
import bok.dask_infra
import bok.pd_infra


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 20)


def reduce_flows_to_pandas(in_path, out_path):
    """Find the first and last active flow time for each user

    in_path must point to a dask dataframe with a time index and user flows
    """
    # Immediately downselect to only needed columns
    flow_frame = bok.dask_infra.read_parquet(in_path)[["user", "end"]]
    flow_frame = flow_frame.reset_index()

    # Find the first and last time the user was active
    first_active = flow_frame.groupby("user").first()[["start"]]
    last_active = flow_frame.groupby("user").last()[["end"]]

    # Merge the first and last times to one frame
    user_range_frame = first_active.merge(last_active,
                                     left_index=True,
                                     right_index=True)

    user_range_frame = user_range_frame.compute()
    bok.pd_infra.clean_write_parquet(user_range_frame, out_path)


def compute_purchase_range_frame():
    """Generates a frame with the range a user has made purchases in the net
    """
    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM").compute()

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


def compute_user_deltas(flow_range_intermediate_file, active_delta_out_file):
    flow_ranges = bok.pd_infra.read_parquet(flow_range_intermediate_file)
    purchase_ranges = compute_purchase_range_frame()

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
        bok.constants.MAX_DATE - user_ranges["earliest"]

    user_ranges["delta_active"] = \
        user_ranges["latest"] - user_ranges["earliest"]

    # Also store deltas as fractional days
    user_ranges["days_since_first_active"] = \
        user_ranges["delta_since_first_active"].dt.total_seconds() / float(86400)  # (seconds per day)

    user_ranges["days_active"] = \
        user_ranges["delta_active"].dt.total_seconds() / float(86400)  # (seconds per day)

    # Drop un-needed intermediate columns
    user_ranges = user_ranges.drop(["start", "end", "first_purchase", "last_purchase"], axis="columns")

    bok.pd_infra.clean_write_parquet(user_ranges, active_delta_out_file)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    flow_source_file = "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    flow_range_file = "scratch/graphs/user_active_deltas"
    delta_out_file = "data/clean/user_active_deltas.parquet"
    reduce_flows_to_pandas(flow_source_file, flow_range_file)
    compute_user_deltas(flow_range_file, delta_out_file)
