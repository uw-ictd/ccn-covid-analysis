import altair as alt
import bok.dask_infra
import bok.pd_infra
import bok.platform
import numpy as np
import pandas as pd


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start")[["protocol", "dest_port", "bytes_up", "bytes_down"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "protocol", "dest_port"]).sum()

    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
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
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "name"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "name"]).sum()

    grouped_flows = grouped_flows.reset_index()

    print(grouped_flows)

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    plot = alt.Chart(grouped_flows).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Fraction of Traffic Per Week(GB)",
                stack="normalize",
                ),
        # shape="direction",
        color="name",
        detail="name",
    ).properties(
        # title="Local Service Use",
        width=500,
    ).save("renders/bytes_per_protocol_trends_normalized.png",
           scale_factor=2
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
    platform = bok.platform.read_config()

    graph_temporary_file = "scratch/graphs/bytes_per_protocol_trends"

    if platform.large_compute_support:
        print("Running compute")
        client = bok.dask_infra.setup_tuned_dask_client(10, platform.max_memory_gigabytes, platform.max_processors)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()
        print("Done with compute")

    if platform.altair_support:
        chart = make_plot(graph_temporary_file)

    print("Done!")
