import altair as alt
import pandas as pd
import dask.dataframe
import dask.distributed
import ipaddress
import os

import bok.dask_infra


def annotate_local(address):
    return not ipaddress.ip_address(address).is_global


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    # Import the dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.
    flows = dask.dataframe.read_parquet("scratch/flows/typical_fqdn_category_TM_DIV_none_INDEX_start/", engine="fastparquet")

    flows["local"] = flows.apply(
        lambda row: annotate_local(row["dest_ip"]),
        axis="columns",
        meta=("local", bool))

    bok.dask_infra.clean_write_parquet(flows, "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start")
