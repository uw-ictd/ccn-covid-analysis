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
    last_purchases = purchases.sort_values("timestamp").groupby("user").last().reset_index()[["user", "timestamp"]]

    purchase_ranges = first_purchases.merge(last_purchases, on="user")

    return purchase_ranges


def compute_user_deltas(flow_range_intermediate_file):
    flow_ranges = bok.pd_infra.read_parquet(flow_range_intermediate_file)
    purchase_ranges = compute_purchase_range_frame()
    print(flow_ranges)
    print(purchase_ranges)


    active_dates["delta_since_first_purchase"] = (bok.constants.MAX_DATE -
                                                  active_dates["timestamp"])

    # Drop users that have been active less than a week.
    users_to_analyze = active_dates.loc[
        active_dates["delta_since_first_purchase"] >= datetime.timedelta(weeks=1)
        ][["user", "delta_since_first_purchase"]]

    aggregated_purchases = purchases.groupby(
        "user"
    )[["amount_idr", "amount_bytes"]].sum().reset_index()

    aggregated_purchases["amount_GB"] = aggregated_purchases["amount_bytes"] * float(1) / (1000 ** 3)
    aggregated_purchases["amount_USD"] = aggregated_purchases["amount_idr"] * bok.constants.IDR_TO_USD

    # Merge in the active times
    aggregated_purchases = aggregated_purchases.merge(users_to_analyze,
                                                      on="user",
                                                      how="inner")
    # Compute USD/Day
    aggregated_purchases["USD_per_day"] = (
            aggregated_purchases["amount_USD"] * 86400 / # (seconds/day)
            aggregated_purchases["delta_since_first_purchase"].dt.total_seconds()
    )

    # Drop un-needed columns since altair cannot handle timedelta types.
    aggregated_purchases = aggregated_purchases[["user", "USD_per_day"]]
    print(aggregated_purchases)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    flow_source_file = "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    flow_range_file = "scratch/graphs/user_active_deltas"
    reduce_flows_to_pandas(flow_source_file, flow_range_file)
    compute_user_deltas(flow_range_file)
