""" Computes revenue earned by the network by month
"""

import altair
import bok.constants
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
max_date = bok.constants.MAX_DATE

def get_month_year(x):
    return x["start"].apply(lambda x_1: datetime.datetime(year=x_1.year, month=x_1.month, day=1), meta=('start', 'datetime64[ns]'))


def get_date(x):
    return x["start"].apply(lambda x_1: datetime.datetime(year=x_1.year, month=x_1.month, day=x_1.day), meta=('start', 'datetime64[ns]'))

def get_revenue_query(transactions):
    # Set down the types for the dataframe
    types = {
        "start": 'datetime64',
        "price": "int64",
    }

    # Update the types in the dataframe
    query = transactions.astype(types)
    # Map each start to a cohort
    query = query.assign(date=get_date)
    # Group by cohorts and get the all the users
    query = query.groupby("date")["price"]
    # Count the number of unique users per cohort
    query = query.sum()
    # Convert query back into a dataframe
    query = query.reset_index()
    # Do a rolling average of the days
    query["rolling_avg"] = query.iloc[:,1].rolling(window=30, win_type="blackman").mean()
    # Drop all NA values
    query = query.dropna(how="any")

    return query

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    flows = bok.dask_infra.read_parquet("data/clean/flows")
    length = len(flows)
    transactions = dask.dataframe.read_csv("data/clean/first_time_user_transactions.csv")
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    print("Processing {} flows".format(length))

    # Get the user data
    revenue = get_revenue_query(transactions)
    # Get the data in a form that is easily plottable
    revenue = revenue.melt(id_vars=["date"], value_vars=["rolling_avg"], var_name="time", value_name="revenue (rupiah)")
    # Reset the types of the dataframe
    types = {
        "date": "datetime64",
        "revenue (rupiah)": "int64"
    }
    revenue = revenue.astype(types)
    # Compute the query
    revenue = revenue.compute()
    print(revenue)

    altair.Chart(revenue).mark_line().encode(
        x="date:T",
        y="revenue (rupiah)",
    ).serve()
    

# Gets the start and end of the date in the dataset. 
def get_date_range():
    client = bok.dask_infra.setup_dask_client()

    flows = dask.dataframe.read_parquet("data/clean/flows", engine="pyarrow")

    # Gets the max date in the flows dataset
    max_date = flows.reset_index()["start"].max()
    max_date = max_date.compute()
    print("max date: ", max_date)
