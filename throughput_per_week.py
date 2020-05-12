""" Computes daily throughput in the network
"""

import altair
import dask.config
import dask.dataframe
import dask.distributed
import datetime
import math
import numpy as np
import pandas as pd

import bok.dask_infra

# Configs
day_intervals = 1
# IMPORTANT: Run get_date_range() to update these values when loading in a new dataset!
max_date = datetime.datetime.strptime('2020-05-03 00:00:00', '%Y-%m-%d %H:%M:%S')

def cohort_as_date(x):
    day = max_date - datetime.timedelta(day_intervals * x)
    return day.strftime("%Y/%m/%d")

def get_cohort(x):
    return x["start"].apply(lambda x_1: (max_date - x_1).days // day_intervals, meta=('start', 'int64'))

def get_date(x):
    return x["cohort"].apply(cohort_as_date, meta=('cohort', 'object'))

def get_throughput_data(flows):
    # Make indexes a column and select "start", "bytes_up", "bytes_down" columns
    query = flows.reset_index()[["start", "bytes_up", "bytes_down"]]
    # Map each start to a cohort
    query = query.assign(cohort=get_cohort)
    # Group by cohorts and get the all the users
    query = query.groupby("cohort")
    # Sum up all of the bytes_up and bytes_down
    query = query.sum()
    # Get the start column back
    query = query.reset_index()
    # Get the total usage per day
    query["total_bytes"] = query["bytes_up"] + query["bytes_down"]
    # Get each date mapped to each cohort
    query = query.assign(date=get_date)

    return query

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows/typical_TM_DIV_none_INDEX_user", engine="fastparquet")
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    # Get the user data
    throughput = get_throughput_data(flows)

    bok.dask_infra.clean_write_parquet(throughput, "scratch/graphs/throughput_per_week")

    throughput = dask.dataframe.read_parquet("scratch/graphs/throughput_per_week",
                                             engine="fastparquet")

    # Get the data in a form that is easily plottable
    throughput = throughput.melt(id_vars=["date"], value_vars=["bytes_up", "bytes_down", "total_bytes"], var_name="throughput_type", value_name="bytes")
    # Reset the types of the dataframe
    types = {
        "date": "object",
        "throughput_type": "category",
        "bytes": "int64"
    }
    throughput = throughput.astype(types)
    # Compute the query
    throughput = throughput.compute()

    altair.Chart(throughput).mark_line().encode(
        x="date",
        y="bytes",
        color="throughput_type",
    ).serve()
    

# Gets the start and end of the date in the dataset. 
def get_date_range():
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows/typical_TM_DIV_none_INDEX_user", engine="fastparquet")
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    # Gets the max date in the flows dataset
    max_date = flows.reset_index()["start"].max()
    max_date = max_date.compute()
    print("max date: ", max_date)
