import altair as alt
import infra.constants
import infra.dask
import infra.pd
import infra.platform
import pandas as pd


def reduce_to_pandas(outfile, dask_client):
    flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start"
    )[["protocol", "dest_port", "bytes_up", "bytes_down"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "protocol", "dest_port"]).sum()

    flows = flows.compute()

    infra.pd.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = infra.pd.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # Map down to a smaller number of protocol names, including "other".
    grouped_flows["name"] = grouped_flows.apply(
        lambda row: _assign_protocol_plain_name(row.protocol,
                                                row.dest_port),
        axis="columns"
    )

    test = grouped_flows
    test = grouped_flows.loc[(grouped_flows["protocol"]==17) & (grouped_flows["name"] == "Other UDP")].groupby("dest_port").sum()

    print(test.sort_values("bytes_total"))

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "bytes_up", "bytes_down", "name"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "name"]).sum()

    grouped_flows = grouped_flows.reset_index()

    print(grouped_flows)

    # Generate an outage annotation overlay
    outage_info = pd.DataFrame([{"start": infra.constants.OUTAGE_START, "end": infra.constants.OUTAGE_END}])
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
    proto_totals = grouped_flows.groupby("name").sum().reset_index()
    legend_sort_order = proto_totals.sort_values("bytes_total", ascending=True).set_index("bytes_total").reset_index()
    sort_list = legend_sort_order["name"].tolist()
    sort_list.reverse()

    # Now get the up and down sorts
    proto_totals = grouped_flows.groupby("name").sum().reset_index()
    sort_down_order = proto_totals.sort_values("bytes_down", ascending=True).set_index("bytes_down").reset_index()
    sort_down_order["order"] = sort_down_order.index
    sort_down_order["direction"] = "Downlink"

    sort_up_order = proto_totals.sort_values("bytes_up", ascending=True).set_index("bytes_up").reset_index()
    sort_up_order["order"] = sort_up_order.index
    sort_up_order["direction"] = "Uplink"

    orders = sort_down_order.append(sort_up_order)

    grouped_flows["Downlink"] = grouped_flows["bytes_down"] / (1000**3)
    grouped_flows["Uplink"] = grouped_flows["bytes_up"] / (1000**3)

    # Melt the dataset for faceting
    links = grouped_flows.melt(
        id_vars=["name", "start_bin"],
        value_vars=["Downlink", "Uplink"],
        var_name="direction",
        value_name="GB"
    ).set_index("name")

    # Merge the sort orders back into the larger dataset
    faceted_flows = links.merge(orders, on=["name", "direction"])

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
            "name",
            title="Protocol (By Total)",
            scale=alt.Scale(scheme="tableau10"),
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
        "renders/bytes_per_protocol_trends_normalized_facet.png",
        scale_factor=2,
    )

    plot = alt.Chart(grouped_flows).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Total Traffic Per Week(GB)",
                ),
        # shape="direction",
        color="name",
        detail="name",
    ).properties(
        # title="Local Service Use",
        width=500,
    ).save("renders/bytes_per_protocol_trends.png",
           scale_factor=2
           )

    return plot


def _assign_protocol_plain_name(proto, port):
    if proto == 6:
        if port == 22:
            return "SSH"
        if port == 25:
            return "SMTP"
        if port == 53:
            return "DNS"
        if port == 80:
            return "HTTP"
        if port == 143:
            return "IMAP"
        if port == 220:
            return "IMAP"
        if port == 443:
            return "HTTPS"
        if port == 993:
            return "IMAP"
        if port == 5349:
            return "STUN/TURN TLS"
        return "Other TCP"
    elif proto == 17:
        if port == 53:
            return "DNS"
        if port == 80:
            return "HTTP/3 (QUIC)"
        if port == 123:
            return "NTP"
        if port == 143:
            return "IMAP"
        if port == 443:
            return "HTTP/3 (QUIC)"
        if port == 3478:
            return "STUN/TURN"
        return "Other UDP"

    return "Other"


if __name__ == "__main__":
    platform = infra.platform.read_config()

    graph_temporary_file = "scratch/graphs/bytes_per_protocol_trends"

    if platform.large_compute_support:
        print("Running compute")
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()
        print("Done with compute")

    if platform.altair_support:
        chart = make_plot(graph_temporary_file)

    print("Done!")
