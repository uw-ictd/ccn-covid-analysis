""" Computes daily throughput in the network
"""

import altair
import infra.constants
import dask.config
import dask.dataframe
import dask.distributed
import datetime
import math
import numpy as np
import pandas as pd

import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    typical = infra.dask.read_parquet("data/clean/flows_typical_DIV_none_INDEX_start")[["bytes_up", "bytes_down"]]
    p_to_p = infra.dask.read_parquet("data/clean/flows_p2p_DIV_none_INDEX_start")[["bytes_b_to_a", "bytes_a_to_b"]]
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

    infra.pd.clean_write_parquet(combined, outfile)


def make_plot(infile):
    throughput = infra.pd.read_parquet(infile)

    # Fix plotting scale
    throughput["GB"] = throughput["bytes"] / (1000**3)

    temp = throughput.copy()
    temp = temp.groupby("day_bin").sum()
    print("Std dev", temp["GB"].std())
    print("Mean", temp["GB"].mean())

    # Generate a dense dataframe with all days and directions
    date_range = pd.DataFrame({"day_bin": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="1D")})
    category_range = pd.DataFrame({"throughput_type": ["Up", "Down", "Local"]}, dtype=object)
    dense_index = infra.pd.cartesian_product(date_range, category_range)

    throughput = dense_index.merge(
        throughput,
        how="left",
        left_on=["day_bin", "throughput_type"],
        right_on=["day_bin", "throughput_type"]
    ).fillna(value={"bytes": 0, "GB": 0})

    throughput_windowed = throughput.set_index("day_bin").sort_index()

    throughput_windowed = throughput_windowed.groupby(
        ["throughput_type"]
    ).rolling(
        window="7D",
    ).mean().reset_index()

    # Work around vega-lite legend merging bug
    label_order = {
        "Down": 1,
        "Up": 2,
        "Local": 3,
    }
    # Mergesort is stablely implemented : )
    throughput = throughput.sort_values(
        ["throughput_type"],
        key=lambda col: col.map(lambda x: label_order[x]),
        kind="mergesort",
    )
    throughput_windowed = throughput_windowed.sort_values(
        ["throughput_type"],
        key=lambda col: col.map(lambda x: label_order[x]),
        kind="mergesort",
    )

    points = altair.Chart(throughput).mark_point(opacity=0.5).encode(
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
        color=altair.Color(
            "throughput_type",
            sort=None,
        ),
        shape=altair.Shape(
            "throughput_type",
            sort=None,
            legend=altair.Legend(
                title="",
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
                columns=3,
            ),
        )
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
        color=altair.Color(
            "throughput_type",
            sort=None,
            legend=None,
        ),
    )

    (points + lines).resolve_scale(
        color='independent',
        shape='independent'
    ).properties(
        width=500
    ).save(
        "renders/bytes_per_week.png", scale_factor=2
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()

    graph_temporary_file = "scratch/graphs/bytes_per_week"
    if platform.large_compute_support:
        print("Running compute tasks")
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
