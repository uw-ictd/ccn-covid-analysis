""" Purchase rate per user, normalized by when they first joined the network.
"""

import altair as alt
import datetime
import pandas as pd

import bok.constants
import bok.dask_infra


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)


def make_rate_chart():
    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM").compute()

    # Each user's total amount of data purchased directly.
    purchases = transactions.loc[transactions["kind"] == "purchase"]

    # Find the first day the user was active. Define "active" as making first
    # purchase.
    active_dates = purchases.sort_values("timestamp").groupby("user").first().reset_index()[["user", "timestamp"]]
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

    alt.Chart(aggregated_purchases).mark_bar().encode(
        x=alt.X('user',
                sort=alt.SortField(field="amount_bytes",
                                   order="descending"
                                   ),
                ),
        y=alt.Y('USD_per_day',
                scale=alt.Scale(type="log"),
                ),
    ).save("renders/rate_purchase_per_user.png", scale_factor=2.0)

    # Compute a CDF since the specific user does not matter
    value_column = "USD_per_day"

    # Find the PDF first
    stats_frame = aggregated_purchases.groupby(value_column).count()[["user"]].rename(columns = {"user": "user_count"})
    stats_frame["pdf"] = stats_frame["user_count"] / sum(stats_frame["user_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()

    stats_frame = stats_frame.reset_index()
    alt.Chart(stats_frame).mark_line().encode(
        x=alt.X('USD_per_day:Q',
                scale=alt.Scale(type="linear"),
                title="Amount Purchased/Days Since Joining (USD/Day)",
                ),
        y=alt.Y('cdf',
                title="CDF of Users N={}".format(len(stats_frame)),
                scale=alt.Scale(domain=[0, 1]),
                ),
    ).save("renders/rate_purchase_per_user_cdf.png", scale_factor=2.0)

    alt.Chart(stats_frame).mark_line().encode(
        x=alt.X('USD_per_day:Q',
                scale=alt.Scale(type="log"),
                title="Amount Purchased/Days Since Joining (USD/Day - Log Scale)",
                ),
        y=alt.Y('cdf',
                title="CDF of Users N={}".format(len(stats_frame)),
                scale=alt.Scale(domain=[0, 1]),
                ),
    ).save("renders/rate_purchase_per_user_cdf_log.png", scale_factor=2.0)


if __name__ == "__main__":
    make_rate_chart()
