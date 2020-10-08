""" Analyze the ratio of uplink and downlink per user
"""

import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.domains
import bok.pd_infra
import bok.platform


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_ul_dl_scatter_plot(infile):
    user_totals = bok.pd_infra.read_parquet(infile)
    user_totals = user_totals.reset_index()
    user_totals["bytes_total"] = user_totals["bytes_up"] + user_totals["bytes_down"]

    # Filter users by time in network to eliminate early incomplete samples
    user_active_ranges = bok.pd_infra.read_parquet(
        "data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active", "days_online"]]
    # Drop users that joined less than a week ago.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] > 7
        ]
    # Drop users active for less than one day
    users_to_analyze = users_to_analyze.loc[
        users_to_analyze["days_active"] > 1,
    ]

    user_totals = user_totals.merge(users_to_analyze, on="user", how="inner")

    # Rank users by their online daily use.
    user_totals["bytes_avg_per_online_day"] = user_totals["bytes_total"] / user_totals["days_online"]
    user_totals["rank_total"] = user_totals["bytes_total"].rank(method="min", pct=False)
    user_totals["rank_daily"] = user_totals["bytes_avg_per_online_day"].rank(method="min", pct=False)

    # Normalize ul and dl by days online
    user_totals["bytes_up_avg_per_online_day"] = user_totals["bytes_up"] / user_totals["days_online"]
    user_totals["bytes_down_avg_per_online_day"] = user_totals["bytes_down"] / user_totals["days_online"]

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    user_totals["normalized_days_online"] = np.minimum(
        user_totals["days_online"], user_totals["days_active"]) / user_totals["days_active"]

    graph_frame = user_totals.melt(id_vars="rank_daily", value_vars=["bytes_up_avg_per_online_day", "bytes_down_avg_per_online_day", "bytes_avg_per_online_day"], var_name="direction", value_name="amount")

    alt.Chart(graph_frame).mark_point().encode(
        x="rank_daily:N",
        y=alt.Y(
            "amount",
        ),
        color=alt.Color(
            "direction:N",
            #scale=alt.Scale(scheme="tableau20"),
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/dl_ul_ratio_per_user_scatter.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()
    graph_temporary_file = "scratch/graphs/dl_ul_ratio_per_user"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        # Module specific format options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_rows', None)
        make_ul_dl_scatter_plot(graph_temporary_file)
        make_ul_dl_cdf(graph_temporary_file)

    print("Done!")
