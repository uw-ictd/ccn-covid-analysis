import pandas as pd
import dask.dataframe
import os

import bok.dask_infra

def shift_flows_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)


        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")

        df = df.reset_index()
        df["start"] = df["start"] + pd.tseries.offsets.DateOffset(hours=9)
        df["end"] = df["end"] + pd.tseries.offsets.DateOffset(hours=9)
        df = df.set_index("start")

        handle = bok.dask_infra.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive flow shift now")
    client.compute(future_handles, sync=True)


def shift_dns_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")

        df = df.reset_index()
        df["timestamp"] = df["timestamp"] + pd.tseries.offsets.DateOffset(hours=9)
        df = df.set_index("timestamp")

        handle = bok.dask_infra.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive dns shift now")
    client.compute(future_handles, sync=True)


def shift_flows_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df["start"] = df["start"] + pd.tseries.offsets.DateOffset(hours=9)
    df["end"] = df["end"] + pd.tseries.offsets.DateOffset(hours=9)
    print("Single layer flow shift now")
    bok.dask_infra.clean_write_parquet(df, out_path)


def shift_dns_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df["timestamp"] = df["timestamp"] + pd.tseries.offsets.DateOffset(hours=9)
    print("Single layer dns shift now")
    bok.dask_infra.clean_write_parquet(df, out_path)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    shift_flows_recursive("data/clean/flows/typical_fqdn_DIV_user_INDEX_start",
                          "data/clean/flows/typical_fqdn_TZ_DIV_user_INDEX_start",
                          client)

    shift_flows_flat_noindex("data/clean/flows/typical_DIV_none_INDEX_user",
                             "data/clean/flows/typical_TZ_DIV_none_INDEX_user")

    shift_dns_recursive("data/clean/dns/successful_DIV_user_INDEX_timestamp",
                        "data/clean/dns/successful_TZ_DIV_user_INDEX_timestamp")

    shift_dns_flat_noindex("data/clean/dns/successful_DIV_none_INDEX_none",
                           "/clean/dns/successful_TZ_DIV_none_INDEX_none",)

