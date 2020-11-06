import pandas as pd
import altair as alt

import infra.constants
import infra.dask
import infra.pd

transactions = infra.pd.read_parquet("data/clean/transactions_TM.parquet")

# Find the first day the user was active. Define "active" as making first
# purchase or first data in network.
user_active_ranges = infra.pd.read_parquet("data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active"]]

# Drop users that have been active less than a week.
users_to_analyze = user_active_ranges.loc[
    user_active_ranges["days_since_first_active"] >= 7,
]

# Drop users active for less than one week
users_to_analyze = users_to_analyze.loc[
    users_to_analyze["days_active"] >=1,
]


# Each user's total amount of data purchased directly.

purchases = transactions.loc[transactions["kind"] == "purchase"]
aggregated_purchases = purchases.groupby("user")

aggregate_frame = aggregated_purchases[["amount_idr", "amount_bytes"]].sum()
aggregate_frame = aggregate_frame.reset_index()

aggregate_frame["amount_GB"] = aggregate_frame["amount_bytes"] * float(1) / (1000 ** 3)
aggregate_frame["amount_USD"] = aggregate_frame["amount_idr"] * infra.constants.IDR_TO_USD

# Merge in the active times
aggregate_frame = aggregate_frame.merge(users_to_analyze, on="user", how="inner")

alt.Chart(aggregate_frame).mark_bar().encode(
    x=alt.X('user',
            sort=alt.SortField(field="amount_bytes",
                               order="descending"
                               ),
            ),
    y=alt.Y('amount_USD',
            scale=alt.Scale(type="log"),
            ),
).save("renders/purchase_total_per_user.png", scale_factor=2.0)

# Compute a CDF since the specific user does not matter
value_column = "amount_USD"

# Find the PDF first
stats_frame = aggregate_frame.groupby(value_column).count()[["user"]].rename(columns = {"user": "user_count"})
stats_frame["pdf"] = stats_frame["user_count"] / sum(stats_frame["user_count"])
stats_frame["cdf"] = stats_frame["pdf"].cumsum()
print(stats_frame)

stats_frame = stats_frame.reset_index()
alt.Chart(stats_frame).mark_line(interpolate="step-after", clip=True).encode(
    x=alt.X('amount_USD:Q',
            scale=alt.Scale(type="linear"),
            title="Total Amount Purchased (USD)"
            ),
    y=alt.Y('cdf',
            title="CDF of Users N={}".format(len(aggregate_frame)),
            ),
).properties(
    width=500,
    height=200,
).save("renders/purchase_total_per_user_cdf.png", scale_factor=2.0)

alt.Chart(stats_frame).mark_line(interpolate="step-after", clip=True).encode(
    x=alt.X('amount_USD:Q',
            scale=alt.Scale(type="log"),
            title="Total Amount Purchased (USD - Log Scale)"
            ),
    y=alt.Y('cdf',
            title="CDF of Users N={}".format(len(aggregate_frame)),
            ),
).properties(
    width=500,
    height=200,
).save("renders/purchase_total_per_user_cdf_log.png", scale_factor=2.0)
