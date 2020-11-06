""" Bytes per online day per user
"""

import altair as alt
import numpy as np
import pandas as pd

import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outpath, dask_client):
    flows = infra.dask.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "bytes_up", "bytes_down"]]

    flows["bytes_total"] = flows["bytes_up"] + flows["bytes_down"]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "user"]).sum()

    flows = flows.reset_index()[["start_bin", "user", "bytes_total"]]
    flows = flows.compute()

    infra.pd.clean_write_parquet(flows, outpath)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def make_plot(inpath):
    flows = infra.pd.read_parquet(inpath)
    flows = flows.reset_index()
    flows["MB"] = flows["bytes_total"] / (1000**2)
    user_total = flows[["user", "MB"]]
    user_total = user_total.groupby(["user"]).sum().reset_index()

    activity = infra.pd.read_parquet("data/clean/user_active_deltas.parquet")

    df = user_total.merge(activity[["user", "days_online", "optimistic_days_online", "days_active"]], on="user")
    df["MB_per_online_day"] = df["MB"] / df["days_online"]
    df["MB_per_active_day"] = df["MB"] / df["days_active"]

    online_cdf_frame = compute_cdf(df, value_column="MB_per_online_day", base_column="user")
    online_cdf_frame = online_cdf_frame.rename(columns={"MB_per_online_day": "MB"})
    online_cdf_frame = online_cdf_frame.assign(type="Online Ratio")

    print(online_cdf_frame)
    print("Online median MB per Day", online_cdf_frame["MB"].median())

    active_cdf_frame = compute_cdf(df, value_column="MB_per_active_day", base_column="user")
    active_cdf_frame = active_cdf_frame.rename(columns={"MB_per_active_day": "MB"})
    active_cdf_frame = active_cdf_frame.assign(type="Active Ratio")

    print(active_cdf_frame)
    print("Active median MB per Day", active_cdf_frame["MB"].median())

    plot_frame = online_cdf_frame.append(active_cdf_frame)

    alt.Chart(plot_frame).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X(
            "MB",
            title="Mean MB per Day",
        ),
        y=alt.Y(
            "cdf",
            title="CDF of Users",
            scale=alt.Scale(type="linear", domain=(0, 1.0)),
        ),
        color=alt.Color(
            "type"
        ),
        strokeDash=alt.StrokeDash(
            "type"
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_online_day_per_user_cdf.png", scale_factor=2.0
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    graph_temporary_file = "scratch/graphs/bytes_per_online_day_per_user"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = infra.dask.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(outpath=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        make_plot(inpath=graph_temporary_file)

    print("Done!")
