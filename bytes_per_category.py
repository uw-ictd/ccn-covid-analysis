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
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["category", "org", "bytes_up", "bytes_down", "protocol", "dest_port"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Explore STUN Traffic
    stun = flows.loc[flows["protocol"] == 17 & flows["dest_port"] == 3478]
    stun = stun[["category", "bytes_up", "bytes_down", "org"]].groupby(["category", "org"]).sum()

    pd.set_option("display.max_rows", 200)
    print(stun.head(200))
    pd.reset_option("display.max_rows")

    # Do the grouping
    flows = flows.groupby(["start_bin", "category", "org"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_category_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Combine Static and Translate categories
    grouped_flows["category"] = grouped_flows["category"].replace("Static", value="Non-video Content")
    grouped_flows["category"] = grouped_flows["category"].replace("Translation", value="API")

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "category", "bytes_up", "bytes_down"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "category"]).sum()

    grouped_flows = grouped_flows.reset_index()

    # Figure out sorting order by total amount.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    sort_order = cat_totals.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = sort_order["category"].tolist()
    sort_list.reverse()
    sort_order["order"] = sort_order.index

    # Merge the sort order back into the larger dataset
    grouped_flows = grouped_flows.merge(sort_order[["category", "order"]], on="category")

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)

    alt.Chart(grouped_flows).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Total Traffic Per Week(GB)",
                #stack="normalize"
                ),
        # shape="direction",
        color=alt.Color(
            "category",
            title="Category (by total)",
            scale=alt.Scale(scheme="tableau20"),
            sort=sort_list,
        ),
        order=alt.Order("order"),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_total_vs_time_stream.png",
        scale_factor=2,
    )

    # Figure out sorting order by upload amount.
    cat_up_totals = grouped_flows.groupby("category").sum().reset_index()
    sort_order = cat_totals.sort_values("bytes_up", ascending=True).set_index("bytes_up").reset_index()
    sort_order["order"] = sort_order.index

    # Merge the sort order back into the larger dataset
    grouped_flows = grouped_flows.drop(columns="order")
    grouped_flows = grouped_flows.merge(sort_order[["category", "order"]], on="category")

    grouped_flows["MB"] = grouped_flows["bytes_up"] / (1000**2)
    alt.Chart(grouped_flows).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(MB):Q",
                title="Upload Traffic Per Week(MB)",
                #stack="normalize"
                ),
        # shape="direction",
        color=alt.Color(
            "category",
            title="Category (by total)",
            scale=alt.Scale(scheme="tableau20"),
            # Keep the colors consistent between the category charts.
            sort=sort_list,
        ),
        order=alt.Order("order"),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_up_vs_time_stream.png",
        scale_factor=2,
    )

    grouped_flows = grouped_flows.melt(id_vars=["category"],
                                       value_vars=["bytes_up", "bytes_down"],
                                       var_name="direction",
                                       value_name="bytes")

    grouped_flows = grouped_flows.groupby(["category", "direction"]).sum().reset_index()
    grouped_flows["bytes"] = grouped_flows["bytes"].replace(0, value=1)
    grouped_flows["bytes"] = grouped_flows["bytes"] / 1000**3

    alt.Chart(grouped_flows).mark_bar(opacity=0.7).encode(
        x=alt.X("category:N",
                title="Category",
                axis=alt.Axis(labels=True),
                sort=sort_list,
                ),
        y=alt.Y("bytes:Q",
                title="Total Traffic(GB)",
                # scale=alt.Scale(
                #     # type="log",
                #     domain=[0, 1000]
                # ),
                stack=None,
                ),
        # shape="direction",
        color=alt.Color(
            "direction",
            title="Type",
            #scale=alt.Scale(scheme="tableau20"),
            # Keep the colors consistent between the category charts.
            #sort=sort_list,
        ),
        #order=alt.Order("order"),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_up_and_down_vs_category.png",
        scale_factor=2,
    )

def compute_stats(infile, dimension):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Consolidate by org
    flows = grouped_flows[["bytes_total", dimension]].groupby([dimension]).sum()

    # Bytes Mapped per org
    flows["MB_total"] = flows["bytes_total"] / 1000**2
    total_megabytes = sum(flows["MB_total"])
    unknown_dns_megabytes = flows["MB_total"]["Unknown (No DNS)"]
    unknown_not_mapped_megabytes = flows["MB_total"]["Unknown (Not Mapped)"]
    amount_mappable = total_megabytes - unknown_dns_megabytes
    amount_mapped = total_megabytes - unknown_dns_megabytes - unknown_not_mapped_megabytes

    print("Stats for {}:".format(dimension))
    print("Total MB:", total_megabytes)
    print("Total Unknown", unknown_dns_megabytes)
    print("Amount Mappable", amount_mappable)
    print("Amount Mapped", amount_mapped)
    print("Fraction unmappable", unknown_dns_megabytes/total_megabytes)
    print("Fraction mapped of total", amount_mapped/total_megabytes)
    print("Fraction mapped of mappable", amount_mapped/amount_mappable)


def make_org_plot(infile):
    """ Generate plots to explore the traffic distribution across organizations
    """
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "org"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "org"]).sum()

    grouped_flows = grouped_flows.reset_index()

    # Group into other category
    number_of_main_categories = 9
    sorted_flows = grouped_flows.groupby("org").sum().sort_values("bytes_total", ascending=False)
    orgs_to_other = sorted_flows.index[number_of_main_categories:]
    number_othered = len(orgs_to_other)
    # Create a separate frame with only the main flows and the aggregated other.
    grouped_with_other = grouped_flows.copy()
    grouped_with_other["org"] = grouped_with_other["org"].replace(orgs_to_other, "Other N={}".format(number_othered))

    # Figure out sorting order by total amount.
    sort_check = grouped_with_other.groupby("org").sum().reset_index()
    sort_order = sort_check.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = sort_order["org"].tolist()
    sort_list.reverse()
    sort_order["order"] = sort_order.index

    # Merge the sort order back into the larger dataset
    grouped_with_other = grouped_with_other.merge(sort_order[["org", "order"]], on="org")

    grouped_with_other["GB"] = grouped_with_other["bytes_total"] / (1000**3)
    alt.Chart(grouped_with_other).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Total Traffic Per Week(GB)",
                ),
        # shape="direction",
        color=alt.Color(
            "org",
            title="Organization",
            scale=alt.Scale(scheme="tableau10"),
            sort=sort_list,
            ),
        order=alt.Order("order"),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_org_weekly_stream_main.png",
        scale_factor=2
    )

    # Create a separate frame for just the other flows
    main_flows = sorted_flows.index[:number_of_main_categories]
    others = grouped_flows.copy().reset_index().set_index("org")
    others = others.drop(main_flows).reset_index()

    # Figure out sorting order by total amount.
    sort_check = others.groupby("org").sum().reset_index()
    sort_order = sort_check.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = sort_order["org"].tolist()
    sort_list.reverse()
    sort_order["order"] = sort_order.index

    # Merge the sort order back into the larger dataset
    others = others.merge(sort_order[["org", "order"]], on="org")

    print(len(others["org"].unique()))
    print(others["org"].unique())
    print(others)

    others["GB"] = others["bytes_total"] / (1000**3)
    alt.Chart(others).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Total Traffic Per Week(GB)",
                #stack="normalize",
                ),
        # shape="direction",
        color=alt.Color(
            "org",
            title="Organization",
            scale=alt.Scale(scheme="category20c"),
            sort=sort_list,
        ),
        # The order actually makes this chart harder to understand, since the color needs to wrap around.
        order=alt.Order("order"),
    ).configure_legend(
        symbolLimit=100,
        columns=2,
    ).properties(
        width=1000,
        height=500,
    ).save(
        "renders/bytes_per_category_org_weekly_stream_others.png",
        scale_factor=2
    )


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/bytes_per_category"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    # make_category_plot(graph_temporary_file)
    # make_org_plot(graph_temporary_file)
    # compute_stats(graph_temporary_file, "org")
    # compute_stats(graph_temporary_file, "category")
