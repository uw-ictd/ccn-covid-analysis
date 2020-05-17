import altair as alt
import bok.dask_infra
import bok.pd_infra
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
    raise NotImplementedError("No graph 4 u")


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/throughput_per_protocol_trends"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    chart = make_plot(graph_temporary_file)
    chart.interactive().serve(port=8891, open_browser=False)
