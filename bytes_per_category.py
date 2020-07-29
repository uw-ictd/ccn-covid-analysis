""" Computing active and registered users on the network over time
"""

import altair as alt
import pandas as pd

import bok.constants
import bok.dask_infra
import bok.pd_infra
import bok.platform


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 40)


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["category", "org", "bytes_up", "bytes_down", "protocol", "dest_port"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "category", "org"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_category_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "category", "bytes_up", "bytes_down"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "category"]).sum()

    grouped_flows = grouped_flows.reset_index()

    # Generate an outage annotation overlay
    outage_info = pd.DataFrame([{"start": bok.constants.OUTAGE_START, "end": bok.constants.OUTAGE_END}])
    outage_annotation = alt.Chart(outage_info).mark_rect(
        opacity=0.7,
        # cornerRadius=2,
        strokeWidth=2,
        # stroke="black"
    ).encode(
        x=alt.X("start"),
        x2=alt.X2("end"),
        color=alt.value("#FFFFFF")
    )

    # Figure out legend sorting order by total amount.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    legend_sort_order = cat_totals.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = legend_sort_order["category"].tolist()
    sort_list.reverse()

    # Now get the up and down sorts
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    sort_down_order = cat_totals.sort_values("bytes_down", ascending=True).set_index("bytes_down").reset_index()
    sort_down_order["order"] = sort_down_order.index
    sort_down_order["direction"] = "Downlink"

    sort_up_order = cat_totals.sort_values("bytes_up", ascending=True).set_index("bytes_up").reset_index()
    sort_up_order["order"] = sort_up_order.index
    sort_up_order["direction"] = "Uplink"

    orders = sort_down_order.append(sort_up_order)

    grouped_flows["Downlink"] = grouped_flows["bytes_down"] / (1000**3)
    grouped_flows["Uplink"] = grouped_flows["bytes_up"] / (1000**3)

    # Melt the dataset for faceting
    links = grouped_flows.melt(
        id_vars=["category", "start_bin"],
        value_vars=["Downlink", "Uplink"],
        var_name="direction",
        value_name="GB"
    ).set_index("category")

    # Merge the sort orders back into the larger dataset
    faceted_flows = links.merge(orders, on=["category", "direction"])

    area = alt.Chart().mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Share of Traffic Per Week",
                stack="normalize"
                ),
        color=alt.Color(
            "category",
            title="Category (By Total)",
            scale=alt.Scale(scheme="tableau20"),
            sort=sort_list,
        ),
        order=alt.Order("order"),
    )

    (area + outage_annotation).properties(
        width=500,
    ).facet(
        column=alt.Column(
            'direction:N',
            title="",
            ),
        data=faceted_flows,
    ).save(
        "renders/bytes_per_category_cat_facet.png",
        scale_factor=2,
    )


def make_category_aggregate_bar_chart(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile).reset_index()

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[
        ["start_bin", "category", "bytes_up", "bytes_down"]
    ].groupby(
        [pd.Grouper(key="start_bin", freq="W-MON"), "category"]
    ).sum().reset_index()

    grouped_flows = grouped_flows.melt(id_vars=["category"],
                                       value_vars=["bytes_up", "bytes_down"],
                                       var_name="direction",
                                       value_name="bytes")

    grouped_flows = grouped_flows.groupby(["category", "direction"]).sum().reset_index()
    grouped_flows["bytes"] = grouped_flows["bytes"].replace(0, value=1)
    grouped_flows["bytes"] = grouped_flows["bytes"] / 1000**3

    alt.Chart(grouped_flows).mark_bar(opacity=1.0).encode(
        x=alt.X("category:N",
                title="Category",
                axis=alt.Axis(labels=True),
                sort='-y',
                ),
        y=alt.Y("bytes:Q",
                title="Total Traffic(GB)",
                # scale=alt.Scale(
                #     # type="log",
                #     domain=[0, 1000]
                # ),
                stack=True,
                ),
        # shape="direction",
        color=alt.Color(
            "direction",
            title="Type",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_bar_up_and_down_vs_category.png",
        scale_factor=2,
    )


def compute_stats(infile, dimension):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    flows = grouped_flows[["bytes_total", dimension]].groupby([dimension]).sum()

    flows["GB_total"] = flows["bytes_total"] / 1000**3
    total_gigabytes = sum(flows["GB_total"])
    unknown_dns_gigabytes = flows["GB_total"]["Unknown (No DNS)"]
    unknown_not_mapped_gigabytes = flows["GB_total"]["Unknown (Not Mapped)"]
    amount_mappable = total_gigabytes - unknown_dns_gigabytes
    amount_mapped = total_gigabytes - unknown_dns_gigabytes - unknown_not_mapped_gigabytes

    print("Stats for {}:".format(dimension))
    print("Total GB:", total_gigabytes)
    print("Total Unknown", unknown_dns_gigabytes)
    print("Amount Mappable", amount_mappable)
    print("Amount Mapped", amount_mapped)
    print("Fraction unmappable", unknown_dns_gigabytes/total_gigabytes)
    print("Fraction mapped of total", amount_mapped/total_gigabytes)
    print("Fraction mapped of mappable", amount_mapped/amount_mappable)


def make_org_plot(infile):
    """ Generate plots to explore the traffic distribution across organizations
    """
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "bytes_up", "bytes_down", "org"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "org"]).sum()

    grouped_flows = grouped_flows.reset_index()

    # Generate an outage annotation overlay
    outage_info = pd.DataFrame([{"start": bok.constants.OUTAGE_START, "end": bok.constants.OUTAGE_END}])
    outage_annotation = alt.Chart(outage_info).mark_rect(
        opacity=0.7,
        # cornerRadius=2,
        strokeWidth=2,
        # stroke="black"
    ).encode(
        x=alt.X("start"),
        x2=alt.X2("end"),
        color=alt.value("#FFFFFF")
    )

    # Group into other orgs
    number_of_main_orgs = 9
    sorted_flows = grouped_flows.groupby("org").sum().sort_values("bytes_total", ascending=False)
    orgs_to_other = sorted_flows.index[number_of_main_orgs:]
    number_othered = len(orgs_to_other)

    # Create a separate frame with only the main flows and the aggregated other.
    grouped_with_other = grouped_flows.copy()
    grouped_with_other["org"] = grouped_with_other["org"].replace(orgs_to_other, "Other N={}".format(number_othered))

    # Group together to find orders for the legend and both areas below.
    org_groups = grouped_with_other.groupby("org").sum().reset_index()

    # Figure out legend sorting order by total amount.
    legend_order = org_groups.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    legend_sort_list = legend_order["org"].tolist()

    # Figure out area layer order by amounts for upload and download.
    sort_order_down = org_groups.sort_values("bytes_down", ascending=True).set_index("bytes_down").reset_index()
    sort_order_down["order"] = sort_order_down.index
    sort_order_down["direction"] = "Downlink"

    sort_order_up = org_groups.sort_values("bytes_up", ascending=True).set_index("bytes_up").reset_index()
    sort_order_up["order"] = sort_order_up.index
    sort_order_up["direction"] = "Uplink"

    area_sort_orders = sort_order_up.append(sort_order_down)

    # Melt the main dataframe
    grouped_with_other["Downlink"] = grouped_with_other["bytes_down"] / (1000**3)
    grouped_with_other["Uplink"] = grouped_with_other["bytes_up"] / (1000**3)
    grouped_with_other = grouped_with_other.melt(
        id_vars=["org", "start_bin"],
        value_vars=["Downlink", "Uplink"],
        var_name="direction",
        value_name="GB"
    )

    # Merge the sort order back into the larger dataset
    grouped_with_other = grouped_with_other.merge(area_sort_orders, on=["org", "direction"])
    print(grouped_with_other)
    area = alt.Chart().mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Share of Traffic Per Week",
                stack="normalize",
                ),
        # shape="direction",
        color=alt.Color(
            "org",
            title="Organization (By Total)",
            scale=alt.Scale(scheme="accent"),
            sort=legend_sort_list,
            ),
        order=alt.Order("order"),
    )

    (area + outage_annotation).properties(
        width=500,
    ).facet(
        column=alt.Column(
            "direction:N",
            title="",
        ),
        data=grouped_with_other,
    ).save(
        "renders/bytes_per_category_org_facet_main.png",
        scale_factor=2
    )

    # Create a separate frame for just the other flows
    main_flows = sorted_flows.index[:number_of_main_orgs]
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
    area = alt.Chart(others).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Total Traffic Per Week(GB)",
                stack="normalize",
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
    )

    (area + outage_annotation).configure_legend(
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
    platform = bok.platform.read_config()

    graph_temporary_file = "scratch/graphs/bytes_per_category"
    if platform.large_compute_support:
        client = bok.dask_infra.setup_tuned_dask_client(10, platform.max_memory_gigabytes, platform.max_processors)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        make_category_plot(graph_temporary_file)
        make_org_plot(graph_temporary_file)
        make_category_aggregate_bar_chart(graph_temporary_file)

    compute_stats(graph_temporary_file, "org")
    compute_stats(graph_temporary_file, "category")
