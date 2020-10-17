""" Computing active and registered users on the network over time
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
        # Module specific format options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_rows', None)

        make_category_plot(graph_temporary_file)
        make_org_plot(graph_temporary_file)
        make_category_plot_separate_top_n(graph_temporary_file)

    print("Done!")
