""" Computing active and registered users on the network over time
"""

import altair
import bok.dask_infra
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
max_date = datetime.datetime.strptime('2020-05-03 00:00:00', '%Y-%m-%d %H:%M:%S')

def cohort_as_date_interval(x):
    cohort_start = max_date - datetime.timedelta(day_intervals * x)
    cohort_end = max_date - datetime.timedelta(day_intervals * x + day_intervals - 1)

    return cohort_start.strftime("%Y/%m/%d") + "-" + cohort_end.strftime("%Y/%m/%d")

def get_cohort(x):
    return x["start"].apply(lambda x_1: (max_date - x_1).days // day_intervals, meta=('start', 'int64'))

def get_date(x):
    return x["cohort"].apply(cohort_as_date_interval, meta=('cohort', 'object'))

def get_active_users_query(flows):
    # Make indexes a column and select "start" and "user" columns
    query = flows.reset_index()[["start", "user"]]
    # Map each start to a cohort
    query = query.assign(cohort=get_cohort)
    # Group by cohorts and get the all the users
    query = query.groupby("cohort")["user"]
    # Count the number of unique users per cohort
    query = query.nunique()
    # Convert to dataframe
    query = query.to_frame()
    # Get the cohort column back
    query = query.reset_index()

    return query

def get_registered_users_query(transactions):
    # Set down the types for the dataframe
    types = {
        'start': 'datetime64[ns]',
        "action": "object",
        "user": "object",
        "amount": "int64",
        "price": "int64"
    }

    # Update the types in the dataframe
    query = transactions.astype(types)
    query = query.set_index("start")
    # Abuse cumsum to get a counter, since the users are already
    # distinct and sorted.
    query = query.assign(temp=1)
    query["count"] = query["temp"].cumsum()
    query = query.drop(["temp"], axis="columns")

    # Compute the number of users at each week, and store in the
    # "user" column
    query = query.resample("1w").last()
    query = query.drop("user", axis="columns").rename(columns={"count": "user"})
    # For weeks that had no new users added, use the total from previous weeks.
    query["user"] = query["user"].fillna(method="ffill")

    # Get the start column back
    query = query.reset_index()
    # Map each start to a cohort
    query = query.assign(cohort=get_cohort)

    # Ignore cohorts that are past the max date
    query = query.query("cohort >= 0")

    return query

def get_user_data(flows, transactions):
    active_users = get_active_users_query(flows)
    registered_users = get_registered_users_query(transactions)

    # Join the active and registered users together
    users = active_users.merge(registered_users,
                               how="outer",
                               left_on="cohort",
                               right_on="cohort",
                               suffixes=('_active', '_registered'))

    # For cohorts with no active users, fill zero.
    users["user_active"] = users["user_active"].fillna(value=0)

    # Map each cohort to a date
    users = users.assign(date_range=get_date)

    return users

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.
    flows = dask.dataframe.read_parquet("data/clean/flows/typical_TM_DIV_none_INDEX_user", engine="fastparquet")

    transactions = dask.dataframe.read_csv("data/clean/first_time_user_transactions.csv")
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    # Get the user data
    users = get_user_data(flows, transactions)
    # Get the data in a form that is easily plottable
    users = users.melt(id_vars=["date_range"], value_vars=["user_active", "user_registered"], var_name="user_type", value_name="num_users")
    # Reset the types of the dataframe
    types = {
        "date_range": "object",
        "user_type": "category",
        "num_users": "int64"
    }
    users = users.astype(types)

    bok.dask_infra.clean_write_parquet(users, "scratch/users-per-week")

    users = dask.dataframe.read_parquet("scratch/users-per-week", engine="fastparquet")
    # Compute the query
    users = users.compute()

    altair.Chart(users).mark_line().encode(
        x="date_range",
        y="num_users",
        color="user_type",
    ).interactive().show()
    


# Gets the start and end of the date in the dataset.
def get_date_range():
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows", engine="pyarrow")
    #length = len(flows)
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    #print("Processing {} flows".format(length))

    # Gets the max date in the flows dataset
    max_date = flows.reset_index()["start"].max()
    max_date = max_date.compute()
    print("max date: ", max_date)
