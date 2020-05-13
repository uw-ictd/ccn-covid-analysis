""" An example to show how to compute values over the whole dataset with dask
"""

import altair
import bok.dask_infra
import dask.dataframe
import dask.distributed
import numpy as np
import pandas as pd

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # TODO(matt9j) This doesn't have the peer to peer flows included!
    flows = dask.dataframe.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start", engine="fastparquet")[["bytes_up", "bytes_down", "local"]]


    # Dask groupby doesn't fully support the pandas grouper
    # https://github.com/dask/dask/issues/5195 , which is needed to do a
    # compound groupby and resample.

    # Instead remap local and nonlocal bytes to distinct columns.
    flows["local_up"] = flows["bytes_up"]
    flows["local_down"] = flows["bytes_down"]
    flows["bytes_down"] = flows["bytes_down"].where(~flows["local"], other=0)
    flows["bytes_up"] = flows["bytes_up"].where(~flows["local"], other=0)
    flows["local_down"] = flows["local_down"].where(flows["local"], other=0)
    flows["local_up"] = flows["local_up"].where(flows["local"], other=0)

    # Resample to bins
    flows = flows.resample("1w").sum()

    # Store aggregate reduction to disk
    bok.dask_infra.clean_write_parquet(flows, "scratch/graphs/local_vs_nonlocal_tput_resample_week")
    flows = dask.dataframe.read_parquet("scratch/graphs/local_vs_nonlocal_tput_resample_week", engine="fastparquet").compute()

    print(flows)
    print(flows.head())

    # Reset the index to a normal column for plotting
    flows = flows.reset_index()

    print("post index reset")
    print(flows)

    # Transform to long form for altair.
    # https://altair-viz.github.io/user_guide/data.html#converting-between-long-form-and-wide-form-pandas
    flows = flows.melt(id_vars=["start"],
                       value_vars=["bytes_up", "bytes_down", "local_up", "local_down"],
                       var_name="direction",
                       value_name="amount",
                       )
    print(flows)
    print(flows.head())

    chart = altair.Chart(flows).mark_line().encode(
        x=altair.X("start", title="Time", axis=altair.Axis(labels=False)),
        y=altair.Y("amount:Q", title="Amount (Bytes)"),
        color="direction",
    ).properties(
        title="Local vs Nonlocal Data"
    ).configure_title(
        fontSize=20,
        font='Courier',
        anchor='start',
        color='gray'
    ).serve(port=8891, open_browser=False)
