""" Compute local vs external throughput, binned by a configurable time interval
"""

import altair as alt
import numpy as np
import pandas as pd

import infra.constants
import infra.dask_infra
import infra.pd_infra
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    flows = infra.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "local"]]

    peer_flows = infra.dask_infra.read_parquet(
        "data/clean/flows/p2p_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a"]]

    # All peer flows are local
    peer_flows["bytes_p2p"] = peer_flows["bytes_a_to_b"] + peer_flows["bytes_b_to_a"]
    peer_flows = peer_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")

    # Dask groupby doesn't fully support the pandas grouper
    # https://github.com/dask/dask/issues/5195 , which is needed to do a
    # compound groupby and resample.
    #
    # Instead remap local and nonlocal bytes to distinct columns.
    flows["local_up"] = flows["bytes_up"]
    flows["local_down"] = flows["bytes_down"]
    flows["bytes_down"] = flows["bytes_down"].where(~flows["local"], other=0)
    flows["bytes_up"] = flows["bytes_up"].where(~flows["local"], other=0)
    flows["local_down"] = flows["local_down"].where(flows["local"], other=0)
    flows["local_up"] = flows["local_up"].where(flows["local"], other=0)

    flows = flows.append(peer_flows, interleave_partitions=True)

    # Resample to bins
    flows = flows.resample("1w").sum()

    # Realize the result
    flows_realized = flows.compute()

    # Store the reduced pandas dataframe for graphing to disk
    infra.pd_infra.clean_write_parquet(flows_realized, outfile)


def make_plot(infile):
    flows = infra.pd_infra.read_parquet(infile)

    # Record 0 for gaps
    flows = flows.fillna(value=0)

    # Reset the index to a normal column for plotting
    flows = flows.reset_index()

    flows = flows.rename(columns={"local_up": "Upload to Local Server",
                                  "local_down": "Download from Local Server",
                                  "bytes_p2p": "Peer to Peer",
                                  })

    # Transform to long form for altair.
    # https://altair-viz.github.io/user_guide/data.html#converting-between-long-form-and-wide-form-pandas
    flows = flows.melt(id_vars=["start"],
                       value_vars=["bytes_up", "bytes_down", "Upload to Local Server", "Download from Local Server", "Peer to Peer"],
                       var_name="direction",
                       value_name="amount",
                       )

    flows = flows.loc[(flows["direction"] == "Upload to Local Server") | (flows["direction"] == "Download from Local Server") | (flows["direction"] == "Peer to Peer")]

    flows["MB"] = flows["amount"] / (1000**2)
    plot = alt.Chart(flows).mark_area().encode(
        x=alt.X("start:T",
                   title="Time",
                   axis=alt.Axis(labels=True),
                   ),
        y=alt.Y("sum(MB):Q",
                   title="Sum of Amount Per Week(MB)",
                   ),
        # shape="direction",
        color="direction",
        detail="direction",
    ).properties(
        # title="Local Service Use",
        width=500,
    ).configure_title(
        fontSize=20,
        font='Courier',
        anchor='start',
        color='gray'
    )

    return plot


def anomaly_flows_reduce_to_pandas(outpath, dask_client):
    anomaly_flows = infra.dask_infra.read_parquet("data/clean/flows/nouser_TM_DIV_none_INDEX_start")
    infra.pd_infra.clean_write_parquet(anomaly_flows.compute(), outpath)


def anomaly_flows_make_plot(inpath):
    anomaly_flows = infra.pd_infra.read_parquet(inpath).reset_index()

    # Classify the anomalies
    anomaly_flows = anomaly_flows.assign(kind="Unknown")
    anomaly_flows["kind"] = anomaly_flows["kind"].mask(
        ((anomaly_flows["b_port"] == 1900) & (anomaly_flows["bytes_b_to_a"] == 0)),
        other="SSDP Query (No Answer)"
    )
    anomaly_flows["kind"] = anomaly_flows["kind"].mask(
        ((anomaly_flows["bytes_b_to_a"] == 0) & (anomaly_flows["kind"] == "Unknown")),
        other="Unanswered"
    )

    # Aggregate by day for plotting
    anomaly_flows["day_bin"] = anomaly_flows["start"].dt.floor("d")
    anomaly_flows = anomaly_flows.groupby(["day_bin", "kind"]).sum()
    anomaly_flows["bytes_total"] = anomaly_flows["bytes_a_to_b"] + anomaly_flows["bytes_b_to_a"]
    anomaly_flows = anomaly_flows.reset_index()

    # Densify the samples with zeros for days with no observed flows
    dense_index = infra.pd_infra.cartesian_product(
        pd.DataFrame({"day_bin": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE)}),
        pd.DataFrame({"kind": anomaly_flows["kind"].unique()})
    )

    anomaly_flows = dense_index.merge(anomaly_flows, on=["day_bin", "kind"], how="left").fillna(0)

    alt.Chart(anomaly_flows).mark_point(opacity=0.5).encode(
        x=alt.X(
            'day_bin:T',
            title="Time"
        ),
        y=alt.Y(
            'bytes_total',
            title="Total Bytes per Day",
        ),
        color=alt.Color(
            "kind:N",
        ),
        shape="kind:N",
    ).save("renders/local_traffic_anomalies.png", scale_factor=2.0)


def p2p_flows_reduce_to_pandas(outpath, dask_client):
    p2p_flows = infra.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")
    infra.pd_infra.clean_write_parquet(p2p_flows.compute(), outpath)


def _canonical_order(a, b):
    """Orders two comparable objects into a deterministic tuple"""
    if a <= b:
        return a, b
    else:
        return b, a


def p2p_flows_make_plot(inpath):
    p2p_flows = infra.pd_infra.read_parquet(inpath).reset_index()

    # Classify the flows
    p2p_flows = p2p_flows.assign(kind="Two-Way")
    p2p_flows["kind"] = p2p_flows["kind"].mask(
        ((p2p_flows["bytes_a_to_b"] == 0) | (p2p_flows["bytes_b_to_a"] == 0)),
        other="One-Way"
    )

    # ToDo Analyze the source of these probing flows
    spray_flows = p2p_flows.loc[p2p_flows["kind"] == "One-Way"]

    p2p_flows = p2p_flows.loc[p2p_flows["kind"] == "Two-Way"]
    p2p_flows["bytes_total"] = p2p_flows["bytes_a_to_b"] + p2p_flows["bytes_b_to_a"]
    p2p_flows["key_x"] = p2p_flows.apply(lambda row: _canonical_order(row["user_a"], row["user_b"])[0], axis=1)
    p2p_flows["key_y"] = p2p_flows.apply(lambda row: _canonical_order(row["user_a"], row["user_b"])[1], axis=1)

    # Aggregate by day for plotting
    p2p_flows["day_bin"] = p2p_flows["start"].dt.floor("d")
    p2p_flows = p2p_flows.groupby(["day_bin", "kind", "key_x", "key_y"]).sum()
    p2p_flows = p2p_flows.reset_index()

    # Densify the samples with zeros for days with no observed flows
    dense_index = infra.pd_infra.cartesian_product(
        pd.DataFrame({"day_bin": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE)}),
        pd.DataFrame({"kind": p2p_flows["kind"].unique()})
    )

    # p2p_flows = dense_index.merge(p2p_flows, on=["day_bin", "kind"], how="left").fillna(0)

    alt.Chart(p2p_flows).mark_point(opacity=0.5).encode(
        x=alt.X(
            'day_bin:T',
            title="Time"
        ),
        y=alt.Y(
            'bytes_total',
            title="Total Bytes per Day",
        ),
        color=alt.Color(
            "key_x:N",
        ),
        shape="kind:N",
    ).save("renders/local_traffic_p2p.png", scale_factor=2.0)

    src_users = p2p_flows.groupby(["kind", "key_x", "key_y"]).agg({"bytes_total": "sum",
                                                                   "day_bin": "count",
                                                                   })
    print(src_users)
    src_users = src_users.reset_index()

    src_users["average_bytes"] = src_users["bytes_total"] / src_users["day_bin"]

    # all_involved_users = p2p_flows["user_a"].append(p2p_flows["user_b"]).unique()
    # dense_user_combinations = bok.pd_infra.cartesian_product(
    #     pd.DataFrame({"key_x": all_involved_users}),
    #     pd.DataFrame({"key_y": all_involved_users})
    # )
    # src_users = dense_user_combinations.merge(src_users, on=["key_x", "key_y"], how="left").fillna(0)

    base = alt.Chart(src_users).encode(
        x=alt.X(
            'key_x:N',
            title="User 1"
        ),
        y=alt.Y(
            'key_y:N',
            title="User 2",
        ),
    )
    heatmap = base.mark_circle().encode(
        color=alt.Color(
            "average_bytes:Q",
            scale=alt.Scale(scheme="viridis"),
        ),
        size=alt.Size(
            "bytes_total:Q",
            scale=alt.Scale(range=[300, 2000]),
        )
    )
    text = base.mark_text(baseline="middle").encode(
        text="day_bin",
        color=alt.condition(
            alt.datum.average_bytes > 5 * (1000**2),
            alt.value("black"),
            alt.value("white"),
        ),
    )

    (heatmap + text).save("renders/local_traffic_p2p_by_user.png", scale_factor=2.0)


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temporary_file = "scratch/graphs/local_vs_nonlocal_tput_resample_week"
    anomaly_temporary_file = "scratch/graphs/local_traffic_anomalies"
    p2p_temporary_file = "scratch/graphs/local_traffic_p2p"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = infra.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        anomaly_flows_reduce_to_pandas(outpath=anomaly_temporary_file, dask_client=client)
        p2p_flows_reduce_to_pandas(outpath=p2p_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        p2p_flows_make_plot(p2p_temporary_file)
        anomaly_flows_make_plot(anomaly_temporary_file)
        chart = make_plot(graph_temporary_file)
        chart.save("renders/local_traffic.png", scale_factor=2)

    print("Done!")
