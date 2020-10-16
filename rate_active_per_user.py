import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.pd_infra
import bok.platform

def reduce_to_pandas(outpath, dask_client):
    flows = bok.dask_infra.read_parquet(
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

    bok.pd_infra.clean_write_parquet(flows, outpath)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def make_plot(inpath):
    flows = bok.pd_infra.read_parquet(inpath)
    flows = flows.reset_index()

    activity = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")

    # Drop users new to the network first active less than a week ago.
    activity = activity.loc[
        activity["days_since_first_active"] >= 7,
    ]
    # Drop users active for less than 1 day
    activity = activity.loc[
        activity["days_active"] >= 1.0,
    ]

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    activity["optimistic_online_ratio"] = np.minimum(
        activity["optimistic_days_online"], activity["days_active"]) / activity["days_active"]

    flows["MB"] = flows["bytes_total"] / (1000**2)
    user_total = flows[["user", "MB"]]
    user_total = user_total.groupby(["user"]).sum().reset_index()
    df = user_total.merge(
        activity[["user", "optimistic_online_ratio", "optimistic_days_online", "days_online"]],
        on="user",
    )
    df["MB_per_online_day"] = df["MB"] / df["days_online"]

    scatter = alt.Chart(df).mark_point().encode(
        x=alt.X(
            "MB_per_online_day",
            title="Mean MB per Day Online",
        ),
        y=alt.Y(
            "optimistic_online_ratio",
            title="Online Ratio",
            scale=alt.Scale(type="linear", domain=(0, 1.0)),
        ),
    )

    regression = scatter.transform_regression(
        "MB_per_online_day",
        "optimistic_online_ratio",
        method="linear",
    ).mark_line(color="orange")

    (scatter + regression).properties(
        width=500,
    ).save(
        "renders/rate_active_per_user.png", scale_factor=2.0
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temporary_file = "scratch/graphs/rate_active_per_user"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = bok.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(outpath=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        make_plot(inpath=graph_temporary_file)

    print("Done!")
