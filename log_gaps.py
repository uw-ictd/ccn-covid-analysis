""" Compute gaps where there are no flow logs in progress

Take advantage of the fact that flowlogs exist in memory before being saved.
Times where there are no logs in progress at all, across any user,
could indicate power or enodeb failures.
"""

import altair
import bok.dask_infra
import bok.pd_infra
import numpy as np
import pandas as pd


def reduce_flow_gaps_to_pandas(outfile, dask_client):
    typical_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "end"]]

    peer_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/p2p_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a", "end"]]

    nouser_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/nouser_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a", "end"]]

    # Compute total bytes and drop intermediates
    typical_flows["total_bytes"] = typical_flows["bytes_up"] + typical_flows["bytes_down"]
    peer_flows["total_bytes"] = peer_flows["bytes_a_to_b"] + peer_flows["bytes_b_to_a"]
    nouser_flows["total_bytes"] = nouser_flows["bytes_a_to_b"] + nouser_flows["bytes_b_to_a"]
    typical_flows = typical_flows.drop(["bytes_up", "bytes_down"], axis="columns")
    peer_flows = peer_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")
    nouser_flows = nouser_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")

    # Combine into one master flow frame
    all_flows = typical_flows.append(
        peer_flows, interleave_partitions=True
    ).append(
        nouser_flows, interleave_partitions=True
    )

    # Sort all flows by time.
    all_flows = all_flows.reset_index().set_index("start")

    # Rolling compute the previous_end column
    all_flows["previous_end"] = all_flows["end"].rolling(
        window=2, center=False
    ).apply(
        lambda val: val[0]
    )

    print(all_flows.head())
    print(all_flows.tail())

    # # Resample to bins
    # flows = flows.resample("1w").sum()
    #
    # # Realize the result
    # flows_realized = flows.compute()
    #
    # # Store the reduced pandas dataframe for graphing to disk
    # bok.pd_infra.clean_write_parquet(flows_realized, outfile)


def make_plot(infile):
    raise NotImplementedError("No plot 4 u")


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/flow_gaps"
    reduce_flow_gaps_to_pandas(outfile=graph_temporary_file, dask_client=client)
    chart = make_plot(graph_temporary_file)
    chart.interactive().serve(port=8891, open_browser=False)
