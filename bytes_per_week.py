""" Computes daily throughput in the network
"""

import altair
import bok.constants
import dask.config
import dask.dataframe
import dask.distributed
import datetime
import math
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.pd_infra
import bok.platform

# Configs
day_intervals = 7
# IMPORTANT: Run get_date_range() to update these values when loading in a new dataset!
max_date = bok.constants.MAX_DATE

def cohort_as_date_interval(x):
    cohort_start = max_date - datetime.timedelta(day_intervals * x + day_intervals - 1)
    cohort_end = max_date - datetime.timedelta(day_intervals * x)

    return cohort_start.strftime("%Y/%m/%d") + "-" + cohort_end.strftime("%Y/%m/%d")

def cohort_as_date(x):
    day = max_date - datetime.timedelta(day_intervals * x)
    return day.strftime("%Y/%m/%d")

def get_cohort(x):
    return x["start"].apply(lambda x_1: (max_date - x_1).days // day_intervals, meta=('start', 'int64'))

def get_date(x):
    return x["cohort"].apply(cohort_as_date_interval, meta=('cohort', 'object'))

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


def reduce_to_pandas(outfile, dask_client):
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["bytes_up", "bytes_down"]]
    p_to_p = bok.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")[["bytes_b_to_a", "bytes_a_to_b"]]
    p_to_p["bytes_local"] = p_to_p["bytes_a_to_b"] + p_to_p["bytes_b_to_a"]
    print(p_to_p)

    combined = typical.append(p_to_p).fillna(0)
    print(combined)

    # Compress to days
    combined["day_bin"] = combined.index.dt.floor("d")
    combined = combined.reset_index().set_index("day_bin")

    # Do the grouping
    combined = combined.groupby(["day_bin"]).sum()

    print("just grouped")
    print(combined)
    combined = combined.reset_index()

    print("just reset")
    print(combined)

    # Get the data in a form that is easily plottable
    combined = combined.rename(columns={"bytes_up": "Up",
                                        "bytes_down": "Down",
                                        "bytes_local": "Local",
                                        })
    combined = combined.melt(id_vars=["day_bin"],
                             value_vars=["Up", "Down", "Local"],
                             var_name="throughput_type",
                             value_name="bytes")

    print("post melt")
    print(combined)

    # Reset the types of the dataframe
    types = {
        "day_bin": "datetime64",
        "throughput_type": "category",
        "bytes": "int64"
    }

    combined = combined.astype(types).compute()

    print("computed", combined)

    bok.pd_infra.clean_write_parquet(combined, outfile)


def make_plot(infile):
    throughput = bok.pd_infra.read_parquet(infile)

    # Fix plotting scale
    throughput["GB"] = throughput["bytes"] / (1000**3)

    throughput_windowed = throughput.set_index("day_bin").sort_index()
    print(throughput_windowed)
    throughput_windowed = throughput_windowed.groupby(
        "throughput_type"
    ).rolling(
        window=7,
        center=True,
    ).mean().reset_index()

    print(throughput_windowed)

    points = altair.Chart(throughput).mark_point(opacity=0.3).encode(
        x=altair.X("day_bin:T",
                   title="Time",
                   axis=altair.Axis(
                       labelSeparation=5,
                       labelOverlap="parity",
                   ),
                   ),
        y=altair.Y("GB:Q",
                   title="GB Total Per Day"
                   ),
        color=altair.Color("throughput_type",
                           sort=["Down", "Up", "Local"]
                           ),
    )

    lines = altair.Chart(throughput_windowed).mark_line().encode(
        x=altair.X("day_bin:T",
                   title="Time",
                   axis=altair.Axis(
                       labelSeparation=5,
                       labelOverlap="parity",
                   ),
                   ),
        y=altair.Y("GB:Q",
                   title="GB Total Per Day"
                   ),
        color=altair.Color("throughput_type",
                           sort=["Down", "Up", "Local"]
                           ),
    )

    (points + lines).properties(
        width=500
    ).save(
        "renders/bytes_per_week.png", scale_factor=2
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()

    graph_temporary_file = "scratch/graphs/bytes_per_week"
    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(7, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
