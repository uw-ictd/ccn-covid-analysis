""" Bytes per category per user
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
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "category", "org", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "category", "org"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_category_per_user_plots(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]
    user_category_total = grouped_flows[["user", "category", "bytes_total"]].groupby(
        ["user", "category"]
    ).sum().reset_index()

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

    # Sort categories by total amount of bytes.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    cat_sort_order = cat_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    cat_sort_order["cat_rank"] = cat_sort_order["bytes_total"].rank(method="min", ascending=False)
    cat_sort_list = cat_sort_order["category"].tolist()

    # Rank users by their daily use.
    user_totals = user_category_total.groupby("user").sum().reset_index()
    user_totals = user_totals.merge(users_to_analyze, on="user", how="inner")
    user_totals["user_total_bytes_avg_online_day"] = user_totals["bytes_total"] / user_totals["days_online"]
    user_totals["user_rank"] = user_totals["user_total_bytes_avg_online_day"].rank(method="min")

    user_category_total = user_category_total.merge(
        user_totals[["user", "user_rank", "days_online", "user_total_bytes_avg_online_day"]],
        on="user",
        how="inner"
    )
    user_category_total = user_category_total.merge(cat_sort_order[["category", "cat_rank"]], on="category", how="inner")
    print(user_category_total)

    user_category_total["bytes_avg_online_day"] = user_category_total["bytes_total"] / user_category_total["days_online"]
    user_category_total["share_of_bytes_avg_online_day"] = \
        user_category_total["bytes_avg_online_day"] / user_category_total["user_total_bytes_avg_online_day"]
    print(user_category_total)

    # This might not be showing exactly what I want to show, since in merging
    # users some users that dominate video could be overrepresented. Maybe
    # want to merge on the fraction of traffic to each part from each user?
    # Are users counted equally or are bytes counted equally...
    alt.Chart(user_category_total[["category", "user_rank", "cat_rank", "bytes_avg_online_day"]]).mark_bar().encode(
        x="user_rank:O",
        y=alt.Y(
            "bytes_avg_online_day",
            stack="normalize",
            sort=cat_sort_list,
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "cat_rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_average_online_day_per_user_bar.png",
        scale_factor=2,
    )

    alt.Chart(user_category_total[["category", "user_rank", "cat_rank", "bytes_avg_online_day"]]).mark_point(
        size=10,
        strokeWidth=2,
    ).encode(
        x="user_rank:O",
        y=alt.Y(
            "bytes_avg_online_day",
            sort=cat_sort_list,
            title="average bytes per online day"
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "cat_rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_average_online_day_per_user_points.png",
        scale_factor=2,
    )

    alt.Chart(user_category_total[["category", "user_rank", "cat_rank", "share_of_bytes_avg_online_day"]]).mark_point(
        size=10,
        strokeWidth=2,
    ).encode(
        x="user_rank:O",
        y=alt.Y(
            "share_of_bytes_avg_online_day",
            sort=cat_sort_list,
            title="share of average bytes per online day"
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "cat_rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/share_of_bytes_per_average_online_day_per_user_points.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()
    graph_temporary_file = "scratch/graphs/bytes_per_category_per_user"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        pd.set_option('display.max_columns', None)
        make_category_per_user_plots(graph_temporary_file)

    print("Done!")
