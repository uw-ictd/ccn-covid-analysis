""" Compute local vs external throughput, binned by a configurable time interval
"""

import altair
import bok.dask_infra
import numpy as np
import pandas as pd

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

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

    # Resample to bins and record 0 for gaps
    flows = flows.resample("1w").sum().fillna(value=0)

    # Store aggregate reduction to disk
    bok.dask_infra.clean_write_parquet(
        flows, "scratch/graphs/local_vs_nonlocal_tput_resample_week")
    flows = bok.dask_infra.read_parquet(
        "scratch/graphs/local_vs_nonlocal_tput_resample_week").compute()

    # Reset the index to a normal column for plotting
    flows = flows.reset_index()

    # Transform to long form for altair.
    # https://altair-viz.github.io/user_guide/data.html#converting-between-long-form-and-wide-form-pandas
    flows = flows.melt(id_vars=["start"],
                       value_vars=["bytes_up", "bytes_down", "local_up", "local_down", "bytes_p2p"],
                       var_name="direction",
                       value_name="amount",
                       )

    chart = altair.Chart(flows).mark_line().encode(
        x=altair.X("start", title="Time", axis=altair.Axis(labels=False)),
        y=altair.Y("amount:Q", title="Amount (Bytes)"),
        color="direction",
        shape="direction",
        detail="direction"
    ).properties(
        title="Local vs Nonlocal Data",
        width=1000,
    ).configure_title(
        fontSize=20,
        font='Courier',
        anchor='start',
        color='gray'
    ).interactive().serve(port=8891, open_browser=False)
