""" Compute local vs external throughput, binned by a configurable time interval
"""

import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.pd_infra
import bok.platform


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "local"]]

    peer_flows = bok.dask_infra.read_parquet(
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
    bok.pd_infra.clean_write_parquet(flows_realized, outfile)


def make_plot(infile):
    flows = bok.pd_infra.read_parquet(infile)

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
    anomaly_flows = bok.dask_infra.read_parquet("data/clean/flows/nouser_TM_DIV_none_INDEX_start")
    bok.pd_infra.clean_write_parquet(anomaly_flows.compute(), outpath)


def anomaly_flows_make_plot(inpath):
    anomaly_flows = bok.pd_infra.read_parquet(inpath).reset_index()

    # Remove unanswered ssdp queries
    print(len(anomaly_flows), "anomaly flows including ssdp queries")
    anomaly_flows = anomaly_flows.loc[~((anomaly_flows["b_port"] == 1900) &
                                        (anomaly_flows["bytes_b_to_a"] == 0))]
    print(len(anomaly_flows), "after removal of ssdp")

    anomaly_flows = anomaly_flows.loc[anomaly_flows["bytes_b_to_a"] != 0]
    print(len(anomaly_flows), "after removal of unanswered flows")

    anomaly_flows["day_bin"] = anomaly_flows["start"].dt.floor("d")
    anomaly_flows = anomaly_flows.groupby(["day_bin"]).sum()
    anomaly_flows["bytes_total"] = anomaly_flows["bytes_a_to_b"] + anomaly_flows["bytes_b_to_a"]
    anomaly_flows = anomaly_flows.reset_index()

    alt.Chart(anomaly_flows).mark_line().encode(
        x=alt.X('day_bin:T',
                title="Time"
                ),
        y=alt.Y('bytes_total',
                title="Total Bytes per Day",
                ),
    ).save("renders/local_traffic_anomalies.png", scale_factor=2.0)


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temporary_file = "scratch/graphs/local_vs_nonlocal_tput_resample_week"
    anomaly_temporary_file = "scratch/graphs/local_traffic_anomalies"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = bok.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        anomaly_flows_reduce_to_pandas(outpath=anomaly_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        anomaly_flows_make_plot(anomaly_temporary_file)
        chart = make_plot(graph_temporary_file)
        chart.save("renders/local_traffic.png", scale_factor=2)

    print("Done!")
