import pandas as pd
import dask.dataframe
import os

import bok.dask_infra

def trim_flows_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")
        df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

        handle = bok.dask_infra.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive flow trim now")
    client.compute(future_handles, sync=True)


def trim_dns_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")
        df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

        handle = bok.dask_infra.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive dns trim now")
    client.compute(future_handles, sync=True)


def trim_flows_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df["start"] >= '2019-03-10 00:00') & (df["start"] < '2020-05-03 00:00')]

    print("Single layer flow trim now")
    print(df)
    bok.dask_infra.clean_write_parquet(df, out_path)


def trim_dns_flat(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

    print("Single layer dns trim now")
    bok.dask_infra.clean_write_parquet(df, out_path)


def trim_transactions_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df["timestamp"] >= '2019-03-10 00:00') & (df["timestamp"] < '2020-05-03 00:00')]
    print("Single layer transaction trim  now")
    bok.dask_infra.clean_write_parquet(df, out_path)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    trim_flows_recursive("data/clean/flows/typical_fqdn_TZ_DIV_user_INDEX_start",
                         "data/clean/flows/typical_fqdn_TM_DIV_user_INDEX_start",
                         client)

    trim_flows_flat_noindex("data/clean/flows/typical_TZ_DIV_none_INDEX_user",
                            "data/clean/flows/typical_TM_DIV_none_INDEX_user")

    trim_dns_recursive("data/clean/dns/successful_TZ_DIV_user_INDEX_timestamp",
                       "data/clean/dns/successful_TM_DIV_user_INDEX_timestamp",
                       client)

    trim_dns_flat("data/clean/dns/successful_TZ_DIV_none_INDEX_timestamp",
                  "data/clean/dns/successful_TM_DIV_none_INDEX_timestamp")

    trim_transactions_flat_noindex("data/clean/transactions_TZ",
                                   "data/clean/transactions_TM")

