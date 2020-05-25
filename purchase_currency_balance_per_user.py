import altair as alt
import pandas as pd

from bok.constants import MAX_DATE, MIN_DATE
import bok.parsers
import bok.dask_infra
import bok.pd_infra


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)


def compute_user_currency_histories():
    """Compute the normalized ledger with running user currency balance.
    """
    # Extract data from the transactions file into a resolved pandas frame
    # Importantly, use the timezone adjusted log but NOT the trimmed log to
    # avoid clipping state from early users.
    transactions = bok.dask_infra.read_parquet(
        "data/clean/transactions_TZ"
    ).compute()

    # Split transfers into positive components for the dest and negative for
    # the source
    transfers = transactions.loc[
        (transactions["kind"] == "user_transfer") |
        (transactions["kind"] == "admin_transfer")
        ].reset_index().drop(["index", "amount_bytes", "kind"], axis="columns")

    user_ledger = transfers[["timestamp", "dest_user", "amount_idr"]]
    user_ledger = user_ledger.rename({"dest_user": "user"}, axis="columns")

    temp_ledger = transfers.loc[:, ["timestamp", "user", "amount_idr"]]
    temp_ledger["amount_idr"] = temp_ledger["amount_idr"] * -1

    user_ledger = user_ledger.append(temp_ledger).reset_index().drop("index", axis="columns")

    # Add topups from the admin as positive user balance
    topups = transactions.loc[
        (transactions["kind"] == "admin_topup")
    ].reset_index().drop(
        ["index", "amount_bytes", "user", "kind"], axis="columns"
    )

    topups = topups.rename({"dest_user": "user"}, axis="columns")

    user_ledger = user_ledger.append(topups).reset_index().drop("index", axis="columns")

    # Add purchases as negative balance
    purchases = transactions.loc[
        (transactions["kind"] == "purchase")
    ].reset_index().drop(["index", "amount_bytes", "dest_user", "kind"], axis="columns")

    purchases["amount_idr"] = -purchases["amount_idr"]

    user_ledger = user_ledger.append(purchases).set_index("timestamp")

    running_user_balance = user_ledger
    running_user_balance = running_user_balance.sort_values("timestamp")
    running_user_balance["balance"] = running_user_balance.groupby("user")["amount_idr"].transform(pd.Series.cumsum)

    # Account for offsets from before logging was added.
    negative_balances = running_user_balance.loc[running_user_balance["balance"] < 0, ["user", "balance"]]
    print("==================================================")
    print("Correcting for negative user balance with {} users".format(
        len(negative_balances["user"].unique())
    ))
    print("==================================================")

    negative_offsets = negative_balances.groupby("user").min().rename(columns={"balance": "offset"})
    negative_offsets["offset"] = -negative_offsets["offset"]
    negative_offsets["corrected"] = True

    running_user_balance = running_user_balance.merge(negative_offsets, left_on="user", right_index=True, how="outer")
    running_user_balance["offset"] = running_user_balance["offset"].fillna(0)
    running_user_balance["corrected"] = running_user_balance["corrected"].fillna(False)
    running_user_balance["balance"] += running_user_balance["offset"]
    running_user_balance = running_user_balance.drop(["offset"], axis="columns")

    # Trim to the reporting date range
    running_user_balance = running_user_balance.loc[
        (running_user_balance.index >= MIN_DATE) & (running_user_balance.index < MAX_DATE)]

    # Remove accidental topup outlier for user
    # 5941e43f1e119acbd273d7dc6e82356e081920a10466afb6a56d8a0856457d5b. It
    # looks like they were accidentally transferred money from the reseller
    # that was then pulled back with an admin transfer. This messes up their
    # active time calculation.
    print(running_user_balance.head())
    running_user_balance = running_user_balance.loc[
        ~((running_user_balance["user"] == "5941e43f1e119acbd273d7dc6e82356e081920a10466afb6a56d8a0856457d5b") &
          (running_user_balance.index > "2020-04-24"))]

    return running_user_balance


def make_time_at_zero_plots(user_balance_frame):
    user_balance_frame = user_balance_frame.reset_index()
    user_balance_frame["next_event_time"] = user_balance_frame.groupby("user")["timestamp"].shift(-1)
    user_balance_frame["next_balance"] = user_balance_frame.groupby("user")["balance"].shift(-1)
    user_balance_frame["duration_at_state"] = user_balance_frame["next_event_time"] - user_balance_frame["timestamp"]

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
    # Do the filter
    user_balance_frame = user_balance_frame.loc[user_balance_frame["user"].isin(users_to_analyze["user"])]

    # For the last dangling entry, assign it a "duration" of zero so it won't
    # contribute to the sum.
    user_balance_frame["duration_at_state"] = user_balance_frame["duration_at_state"].fillna(pd.Timedelta(seconds=0))

    # Need to explicitly allow for non-neumeric aggregation for now.
    # https://github.com/pandas-dev/pandas/issues/17382
    user_time_at_zero = user_balance_frame.loc[
        user_balance_frame["balance"] <= 0
    ].groupby("user").sum(numeric_only=False)

    user_time_nonzero = user_balance_frame.loc[
        user_balance_frame["balance"] > 0
    ].groupby("user").sum(numeric_only=False)

    # Cleanup generated frames
    user_time_at_zero = user_time_at_zero.reset_index().rename(columns={"duration_at_state": "duration_at_zero"})
    user_time_nonzero = user_time_nonzero.reset_index().rename(columns={"duration_at_state": "duration_nonzero"})

    # Downselect and merge
    user_time_at_zero = user_time_at_zero.loc[:, ["user", "duration_at_zero", "corrected"]]
    user_time_nonzero = user_time_nonzero.loc[:, ["user", "duration_nonzero", "corrected"]]
    user_timeshare = user_time_nonzero.merge(user_time_at_zero, on="user", how="outer")

    print(user_timeshare.head())

    # Some users never go to zero. Assign them 0 duration in the zero state.
    user_timeshare["duration_at_zero"] = user_timeshare["duration_at_zero"].fillna(pd.Timedelta(seconds=0))

    # Convert to fractional days
    user_timeshare["days_at_zero"] = \
        user_timeshare["duration_at_zero"].dt.total_seconds() / float(86400)  # (seconds per day)

    # Merge in active times
    user_timeshare = user_timeshare.merge(user_active_ranges, on="user", how="inner")

    # Recover the corrected annotation
    user_corrected_status = user_balance_frame.loc[:, ["corrected", "user"]].drop_duplicates()
    user_timeshare = user_timeshare.merge(user_corrected_status, on="user", how="inner")

    print(user_timeshare.head())

    print(len(user_timeshare))

    # Simplify and trim data now for CDF plotting!
    df = user_timeshare.loc[:, ["user", "days_at_zero", "days_active", "corrected", "days_since_first_active"]]
    df["fraction_at_zero"] = df["days_at_zero"] / df["days_active"]

    print(df.loc[df["fraction_at_zero"] > 1])

    alt.Chart(df).mark_point().encode(
        x=alt.X(
            "days_active",
            title="Days Active",
        ),
        y=alt.Y(
            "fraction_at_zero",
            title="Fraction of Total Time with 0 Credit Balance",
        ),
        color="corrected",
    ).save("renders/purchase_currency_balance_per_user.png", scale_factor=2.0)

    df = compute_cdf(df, "fraction_at_zero", "user")

    alt.Chart(df).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X('fraction_at_zero:Q',
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                title="Fraction of Time with 0 Credit Balance"
                ),
        y=alt.Y('cdf',
                title="Fraction of Users (CDF)",
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
    ).save("renders/purchase_currency_balance_per_user_cdf.png", scale_factor=2.0)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


if __name__ == "__main__":
    running_user_balances = compute_user_currency_histories()
    make_time_at_zero_plots(running_user_balances)
