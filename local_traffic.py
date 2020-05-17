""" Compute local vs external throughput, binned by a configurable time interval
"""

import altair
import bok.dask_infra
import bok.pd_infra
import numpy as np
import pandas as pd


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start"
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
    plot = altair.Chart(flows).mark_area().encode(
        x=altair.X("start:T",
                   title="Time",
                   axis=altair.Axis(labels=True),
                   ),
        y=altair.Y("sum(MB):Q",
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


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/local_vs_nonlocal_tput_resample_week"
    # reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    chart = make_plot(graph_temporary_file)
    chart.save("renders/local_traffic.png", scale_factor=2)
