""" Computing active and registered users on the network over time
"""

import altair
import bok.constants
import bok.dask_infra
import bok.pd_infra
import bok.platform
import dask.config
import dask.dataframe
import dask.distributed
import datetime
import math
import numpy as np
import pandas as pd

# Configs
day_intervals = 7
# IMPORTANT: Run get_data_range() to update these values when loading in a new dataset!
max_date = bok.constants.MAX_DATE

def cohort_as_date_interval(x):
    cohort_start = max_date - datetime.timedelta(day_intervals * x + day_intervals - 1)
    cohort_end = max_date - datetime.timedelta(day_intervals * x)

    return cohort_start.strftime("%Y/%m/%d") + "-" + cohort_end.strftime("%Y/%m/%d")

def get_cohort(x):
    return x["start"].apply(lambda x_1: (max_date - x_1).days // day_intervals, meta=('start', 'int64'))

def get_date(x):
    return x["cohort"].apply(cohort_as_date_interval, meta=('cohort', 'object'))


def get_registered_users_query(transactions):
    # Set down the types for the dataframe
    types = {
        'start': 'datetime64[ns]',
        "user": "object",
    }

    # Update the types in the dataframe
    query = transactions.astype(types)[["start", "user"]].copy()
    query = query.set_index("start")
    # Abuse cumsum to get a counter, since the users are already
    # distinct and sorted.
    query = query.assign(temp=1)
    query["count"] = query["temp"].cumsum()
    query = query.drop(["temp"], axis="columns")

    # Compute the number of users at each week, and store in the
    # "user" column
    query = query.drop("user", axis="columns").rename(columns={"count": "user"})
    # For weeks that had no new users added, use the total from previous weeks.
    query["user"] = query["user"].fillna(method="ffill")

    # Get the start column back
    query = query.reset_index()
    # Map each start to a cohort
    query["day"] = query["start"].dt.floor("d")

    return query


def reduce_to_pandas(outfile, dask_client):
    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.
    df = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")

    df = df.reset_index()
    df["day"] = df["start"].dt.floor("d")

    # Group by cohorts and get the all the users
    df = df.groupby(["day", "user"]).sum().reset_index()
    df = df[["day", "user"]]

    bok.pd_infra.clean_write_parquet(df.compute(), outfile)


def make_plot(infile):
    transactions = pd.read_csv("data/clean/first_time_user_transactions.csv")[["start", "user"]]
    registered_users = get_registered_users_query(transactions)

    # Generate a dense dataframe with all days
    date_range = pd.DataFrame({"day": pd.date_range(bok.constants.MIN_DATE, bok.constants.MAX_DATE, freq="1D")})
    registered_users = date_range.merge(
        registered_users,
        how="left",
        left_on="day",
        right_on="day",
    ).fillna(method="ffill").dropna()

    user_days = bok.pd_infra.read_parquet(infile)

    active_users = user_days.groupby("day")["user"].nunique()
    active_users = active_users.to_frame().reset_index()

    # Group weekly to capture the total number of unique users across the entire week and account for intermittent use.
    weekly_users = user_days.groupby(pd.Grouper(key="day", freq="W-MON"))["user"].nunique()
    weekly_users = weekly_users.to_frame().reset_index().rename(columns={"user": "week_unique_users"})
    week_range = pd.DataFrame({"day": pd.date_range(bok.constants.MIN_DATE, bok.constants.MAX_DATE, freq="W-MON")})
    weekly_users = weekly_users.merge(week_range, on="day", how="outer")
    weekly_users.fillna(0)
    print(weekly_users)

    # Join the active and registered users together
    users = active_users.merge(registered_users,
                               how="right",
                               left_on="day",
                               right_on="day",
                               suffixes=('_active', '_registered'))
    users = users.merge(weekly_users, how="outer", on="day")

    # For cohorts with no active users, fill zero.
    users["user_active"] = users["user_active"].fillna(value=0)

    users = users.rename(columns={"day": "date", "user_active": "Unique Daily Active", "user_registered": "Registered", "week_unique_users": "Unique Weekly Active"})
    users = users.set_index("date").sort_index()
    users["Registered"] = users["Registered"].fillna(method="ffill")
    users["Unique Weekly Active"] = users["Unique Weekly Active"].fillna(method="bfill")
    users = users.reset_index()
    print(users)

    # Limit graphs to the study period
    users = users.loc[users["date"] < bok.constants.MAX_DATE]

    # Compute a rolling average
    users["Active 7-Day Average"] = users["Unique Daily Active"].rolling(
        window=7,
    ).mean()

    # Get the data in a form that is easily plottable
    users = users.melt(id_vars=["date"], value_vars=["Unique Daily Active", "Registered", "Unique Weekly Active"], var_name="user_type", value_name="num_users")
    # Drop the rolling average... it wasn't useful
    # users = users.melt(id_vars=["date"], value_vars=["Active", "Registered", "Active 7-Day Average", "Unique Weekly Active"], var_name="user_type", value_name="num_users")
    # Reset the types of the dataframe
    types = {
        "date": "datetime64",
        "user_type": "category",
        "num_users": "int64"
    }
    # Required since some rolling average entries are NaN before the average window is filled.
    users = users.dropna()
    users = users.astype(types)

    users = users.set_index("date").sort_values(["date", "num_users"])
    users = users.reset_index()

    altair.Chart(users).mark_line(interpolate='step-after').encode(
        x=altair.X("date:T",
                   title="Time",
                   axis=altair.Axis(
                       labelSeparation=5,
                       labelOverlap="parity",
                   ),
                   ),
        y=altair.Y("num_users",
                   title="User Count",
                   ),
        color=altair.Color(
            "user_type",
            title="",
            sort=None,
            legend=altair.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=10,
                strokeColor="black",
            ),
       ),
        strokeDash=altair.StrokeDash(
            "user_type",
            sort=None,
        ),
    ).properties(width=500).save("renders/users_per_week.png", scale_factor=2)


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    graph_temporary_file = "scratch/graphs/users_per_week"
    if platform.large_compute_support:
        print("Running compute tasks")
        print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
