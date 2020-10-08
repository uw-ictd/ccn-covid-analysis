""" Computing active and registered users on the network over time
"""

import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.domains
import bok.pd_infra
import bok.platform


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "category", "org", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "category", "org"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_category_plot(infile):
    pd.set_option('display.max_columns', None)
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Figure out sorting order by total amount.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    cat_sort_order = cat_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    cat_sort_list = cat_sort_order["category"].tolist()

    user_totals = grouped_flows.groupby("user").sum().reset_index()
    user_sort_order = user_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    user_sort_list = user_sort_order["user"].tolist()

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    grouped_flows = grouped_flows[["category", "user", "GB"]].groupby(["user", "category"]).sum()
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["logGB"] = grouped_flows["GB"].transform(np.log10)

    # Filter users by time in network to eliminate early incomplete samples
    user_active_ranges = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active"]]
    # Drop users that joined less than a week ago or were active for less than a week.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] >= 7
        ]

    # Only needed if using the time normalized graph.
    # # Drop users active for less than one week
    # users_to_analyze = users_to_analyze.loc[
    #     users_to_analyze["days_active"] >= 7,
    # ]

    grouped_flows = grouped_flows.merge(users_to_analyze, on="user", how="inner")

    alt.Chart(grouped_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total GB)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("category:N",
                title="Category (Sorted by Total GB)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "logGB:Q",
            title="log(Total GB)",
            scale=alt.Scale(scheme="viridis"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category.png",
        scale_factor=2,
    )

    # Normalize by each user's total spend to highlight categories
    user_total_to_merge = user_totals[["user", "bytes_total"]].rename(columns={"bytes_total": "user_total_bytes"})
    normalized_user_flows = grouped_flows.copy()
    normalized_user_flows = normalized_user_flows.merge(user_total_to_merge, on="user")
    normalized_user_flows["user_total_bytes"] = normalized_user_flows["user_total_bytes"] / 1000**3
    normalized_user_flows["normalized_bytes"] = normalized_user_flows["GB"]/normalized_user_flows["user_total_bytes"]

    alt.Chart(normalized_user_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total GB)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("category:N",
                title="Category (Sorted by Total GB)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "normalized_bytes:Q",
            title="Normalized (Per User) Traffic",
            scale=alt.Scale(scheme="viridis"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_normalized_user_total.png",
        scale_factor=2,
    )

    # Normalize by each user's time in network to better compare users
    time_normalized_flows = grouped_flows
    time_normalized_flows["MB_per_day"] = time_normalized_flows["GB"] * 1000 / time_normalized_flows["days_active"]
    time_normalized_flows["log_MB_per_day"] = time_normalized_flows["MB_per_day"].transform(np.log10)

    alt.Chart(time_normalized_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("category:N",
                title="Category (Sorted by Total)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "log_MB_per_day:Q",
            title="MB per Day (Log Transformed)",
            scale=alt.Scale(scheme="viridis"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_normalized_time.png",
        scale_factor=2,
    )


def make_category_plot_separate_top_n(infile, n_to_separate=20):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Figure out sorting order by total amount.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    cat_sort_order = cat_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    cat_sort_list = cat_sort_order["category"].tolist()

    user_totals = grouped_flows.groupby("user").sum().reset_index()
    user_sort_order = user_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    user_sort_list = user_sort_order["user"].tolist()

    # Generate a frame from the sorted user list that identifies the top users
    top_annotation_frame = user_sort_order[["user"]]
    bottom_n = len(user_sort_order) - n_to_separate
    top_annotation_frame = top_annotation_frame.assign(topN="Bottom {}".format(bottom_n))
    top_annotation_frame.loc[top_annotation_frame.index < n_to_separate, "topN"] = "Top {}".format(n_to_separate)

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    grouped_flows = grouped_flows[["category", "user", "GB"]].groupby(["user", "category"]).sum()
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["logGB"] = grouped_flows["GB"].transform(np.log10)
    grouped_flows = grouped_flows.merge(top_annotation_frame, on="user")

    alt.Chart(grouped_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total GB)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("category:N",
                title="Category (Sorted by Total GB)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "GB:Q",
            title="Total GB",
            scale=alt.Scale(scheme="viridis"),
            ),
    ).facet(
        column=alt.Column(
            "topN:N",
            sort="descending",
            title="",
        ),
    ).resolve_scale(
        x="independent",
        color="independent"
    ).save(
        "renders/users_per_category_split_outliers.png",
        scale_factor=2,
    )


def make_org_plot(infile):
    """ Generate plots to explore the traffic distribution across organizations
    """
    pd.set_option('display.max_columns', None)
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # If any orgs are visited by fewer than 5 participants, need to be "other" per IRB
    user_count = grouped_flows.copy()[["org", "user", "bytes_total"]]
    user_count = user_count.set_index("bytes_total")
    user_count = user_count.drop(0).reset_index()
    user_count = user_count.groupby(["org", "user"]).sum().reset_index()
    user_count = user_count.groupby(["org"]).count()
    small_orgs = user_count.loc[user_count["user"] < 5]
    small_orgs = small_orgs.reset_index()["org"]

    grouped_flows = grouped_flows.replace(small_orgs.values, value="Aggregated (Users < 5)")

    # Filter users by time in network to eliminate early incomplete samples
    user_active_ranges = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active"]]
    # Drop users that joined less than a week ago or were active for less than a week.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] >= 7
        ]

    grouped_flows = grouped_flows.merge(users_to_analyze, on="user", how="inner")
    print(user_active_ranges.head(10))

    # Figure out sorting order by total amount.
    org_totals = grouped_flows.groupby("org").sum().reset_index()
    org_sort_order = org_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    cat_sort_list = org_sort_order["org"].tolist()

    user_totals = grouped_flows.groupby("user").sum().reset_index()
    user_sort_order = user_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    user_sort_list = user_sort_order["user"].tolist()

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    grouped_flows = grouped_flows[["org", "user", "GB"]].groupby(["user", "org"]).sum()
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["logGB"] = grouped_flows["GB"].transform(np.log10)

    alt.Chart(grouped_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total GB)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("org:N",
                title="Organization (Sorted by Total GB)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "logGB:Q",
            title="log(Total GB)",
            scale=alt.Scale(scheme="viridis"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_org.png",
        scale_factor=2,
    )

    # Normalize by each user's total spend to highlight categories
    user_total_to_merge = user_totals[["user", "bytes_total"]].rename(columns={"bytes_total": "user_total_bytes"})
    normalized_user_flows = grouped_flows.copy()
    normalized_user_flows = normalized_user_flows.merge(user_total_to_merge, on="user")
    normalized_user_flows["user_total_bytes"] = normalized_user_flows["user_total_bytes"] / 1000**3
    normalized_user_flows["normalized_bytes"] = normalized_user_flows["GB"]/normalized_user_flows["user_total_bytes"]

    alt.Chart(normalized_user_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User (Sorted by Total GB)",
                axis=alt.Axis(labels=False),
                sort=user_sort_list,
                ),
        y=alt.Y("org:N",
                title="Organization (Sorted by Total GB)",
                sort=cat_sort_list,
                ),
        # shape="direction",
        color=alt.Color(
            "normalized_bytes:Q",
            title="Normalized (Per User) Traffic",
            scale=alt.Scale(scheme="viridis"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_org_normalized.png",
        scale_factor=2,
    )


def make_category_quantiles_plots(infile):
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
    cat_sort_order["rank"] = cat_sort_order["bytes_total"].rank(method="min", ascending=False)
    cat_sort_list = cat_sort_order["category"].tolist()

    # Group users by quantiles of their daily use.
    user_totals = user_category_total.groupby("user").sum().reset_index()
    user_totals = user_totals.merge(users_to_analyze, on="user", how="inner")
    user_totals["avg_daily_bytes"] = user_totals["bytes_total"] / user_totals["days_online"]
    user_totals["rank_total"] = user_totals["bytes_total"].rank(method="min", pct=True)

    user_totals["rank_daily"] = user_totals["avg_daily_bytes"].rank(method="min", pct=True)
    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    user_totals["normalized_days_online"] = np.minimum(
        user_totals["days_online"], user_totals["days_active"]) / user_totals["days_active"]

    user_totals["quantile"] = pd.cut(user_totals["rank_daily"], 10)

    # Merge the user quantile information back into the flows, and then group by category
    quantile_flows = user_category_total.merge(user_totals[["user", "quantile", "days_online"]], on="user", how="inner")
    quantile_totals = quantile_flows.groupby(["quantile", "category"]).sum()
    quantile_totals = quantile_totals.reset_index()
    quantile_totals["quantile_str"] = quantile_totals["quantile"].apply(lambda x: str(x))
    # Weights users who were online a lot of days more heavily than directly averaging the totals
    quantile_totals["normalized_bytes_total"] = quantile_totals["bytes_total"] / quantile_totals["days_online"]
    quantile_totals = quantile_totals.merge(cat_sort_order[["category", "rank"]], on="category", how="inner")

    # This might not be showing exactly what I want to show, since in merging
    # users some users that dominate video could be overrepresented. Maybe
    # want to merge on the fraction of traffic to each part from each user?
    # Are users counted equally or are bytes counted equally...
    alt.Chart(quantile_totals[["category", "quantile_str", "bytes_total", "rank", "normalized_bytes_total"]]).mark_bar().encode(
        x="quantile_str:N",
        y=alt.Y(
            "normalized_bytes_total",
            stack="normalize",
            sort=cat_sort_list,
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_average_online_day_per_quantile_bar.png",
        scale_factor=2,
    )

    # This might not be showing exactly what I want to show, since in merging
    # users some users that dominate video could be overrepresented. Maybe
    # want to merge on the fraction of traffic to each part from each user?
    # Are users counted equally or are bytes counted equally...
    alt.Chart(quantile_totals[["category", "quantile_str", "bytes_total", "rank", "normalized_bytes_total"]]).mark_line().encode(
        x="quantile_str:N",
        y=alt.Y(
            "normalized_bytes_total",
            sort=cat_sort_list,
            title="average bytes per online day"
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_average_online_day_per_quantile_line.png",
        scale_factor=2,
    )


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

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    user_totals["normalized_days_online"] = np.minimum(
        user_totals["days_online"], user_totals["days_active"]) / user_totals["days_active"]

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
    graph_temporary_file = "scratch/graphs/users_per_category"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        pd.set_option('display.max_columns', None)
        make_category_quantiles_plots(graph_temporary_file)
        make_category_per_user_plots(graph_temporary_file)

        # make_category_plot(graph_temporary_file)
        # make_org_plot(graph_temporary_file)
        # make_category_plot_separate_top_n(graph_temporary_file)

    print("Done!")
