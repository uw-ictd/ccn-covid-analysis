""" Computing active and registered users on the network over time
"""

import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.domains
import bok.pd_infra


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
    print(normalized_user_flows)
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
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_normalized.png",
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
    print(user_count)
    small_orgs = user_count.loc[user_count["user"] < 5]
    small_orgs = small_orgs.reset_index()["org"]
    print(small_orgs)
    grouped_flows = grouped_flows.replace(small_orgs.values, value="Aggregated (Users < 5)")
    print(grouped_flows)

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
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category_org_normalized.png",
        scale_factor=2,
    )



if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/users_per_category"
    # reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    # make_category_plot(graph_temporary_file)
    make_org_plot(graph_temporary_file)
