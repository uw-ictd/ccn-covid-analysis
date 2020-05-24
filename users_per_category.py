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
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    print(grouped_flows)

    # Figure out sorting order by total amount.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    sort_order = cat_totals.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = sort_order["category"].tolist()
    sort_list.reverse()
    sort_order["order"] = sort_order.index

    # Merge the sort order back into the larger dataset
    grouped_flows = grouped_flows.merge(sort_order[["category", "order"]], on="category")

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)

    alt.Chart(grouped_flows).mark_rect().encode(
        x=alt.X("user:N",
                title="User",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("category:N",
                title="Category",
                #stack="normalize"
                ),
        # shape="direction",
        color=alt.Color(
            "GB:Q",
            title="GB Total",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/users_per_category.png",
        scale_factor=2,
    )


def make_org_plot(infile):
    """ Generate plots to explore the traffic distribution across organizations
    """
    pass


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/users_per_category"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    make_category_plot(graph_temporary_file)
    make_org_plot(graph_temporary_file)
