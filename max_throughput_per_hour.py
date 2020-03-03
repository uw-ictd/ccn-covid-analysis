""" Computes max hourly throughput in the network by day
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
day_intervals = 1
seconds_in_hour = 60 * 60
seconds_in_day = seconds_in_hour * 24
hour_intervals = 1
seconds_intervals = hour_intervals * seconds_in_hour
# IMPORTANT: Run get_date_range() to update these values when loading in a new dataset!
max_date = datetime.datetime.strptime('2020-02-13 21:29:54', '%Y-%m-%d %H:%M:%S')

def date_to_hour_cohort(x):
    diff = max_date - x
    duration = seconds_in_day * diff.days + diff.seconds

    return duration // seconds_intervals

def get_hour_cohort(x):
    return x["start"].apply(date_to_hour_cohort, meta=('start', 'int64'))

def get_day_cohort(x):
    return x["hour_cohort"].apply(hour_cohort_to_day_cohort, meta=('start', 'object'))

def hour_cohort_to_day_cohort(x):
    day = max_date - datetime.timedelta(seconds=x * seconds_in_hour)
    return day.strftime("%Y/%m/%d")

def get_throughput_data(flows):
    # Make indexes a column and select "start", "bytes_up", "bytes_down" columns
    query = flows.reset_index()[["start", "bytes_up", "bytes_down"]]
    # Map each start to a cohort
    query = query.assign(hour_cohort=get_hour_cohort)
    # Group by cohorts and get the all the users
    query = query.groupby("hour_cohort")
    # Sum up all of the bytes_up and bytes_down
    query = query.sum()
    # Get the total usage per hour
    query["total_bytes"] = query["bytes_up"] + query["bytes_down"]
    # Get hour_cohort back
    query = query.reset_index()
    # Select only the relevant columns
    query = query[["hour_cohort", "total_bytes"]]
    # Get each date mapped to each cohort
    query = query.assign(date=get_day_cohort)
    # Group by the day
    query = query.groupby("date")
    # Find the maximum throughput per hour by day
    query = query["total_bytes"].max()
    # Get the date column back
    query = query.reset_index()

    return query

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows", engine="pyarrow")
    length = len(flows)
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    print("Processing {} flows".format(length))

    # Get the user data
    throughput = get_throughput_data(flows)
    # Get the data in a form that is easily plottable
    throughput = throughput.melt(id_vars=["date"], value_vars=["total_bytes"], var_name="max_throughput_by_hour", value_name="bytes")
    # Reset the types of the dataframe
    types = {
        "date": "object",
        "max_throughput_by_hour": "category",
        "bytes": "int64"
    }
    throughput = throughput.astype(types)
    # Compute the query
    throughput = throughput.compute()
    print(throughput)

    altair.Chart(throughput).mark_line().encode(
        x="date",
        y="bytes",
        color="max_throughput_by_hour",
    ).serve()
    

# Gets the start and end of the date in the dataset. 
def get_date_range():
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows", engine="pyarrow")
    length = len(flows)
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    print("Processing {} flows".format(length))

    # Gets the max date in the flows dataset
    max_date = flows.reset_index()["start"].max()
    max_date = max_date.compute()
    print("max date: ", max_date)
