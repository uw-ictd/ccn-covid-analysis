import pandas as pd
import dask.dataframe
import os

import infra.dask


def shift_flows(in_path, out_path, offset_hours):
    df = infra.dask.read_parquet(in_path)

    df = df.reset_index()
    df["start"] = df["start"] + pd.tseries.offsets.DateOffset(hours=offset_hours)
    df["end"] = df["end"] + pd.tseries.offsets.DateOffset(hours=offset_hours)
    df = df.set_index("start")

    return infra.dask.clean_write_parquet(df, out_path, compute=False)


def shift_dns(in_path, out_path, offset_hours):
    df = infra.dask.read_parquet(in_path)
    df = df.reset_index()
    df["timestamp"] = df["timestamp"] + pd.tseries.offsets.DateOffset(hours=offset_hours)
    df = df.set_index("timestamp")

    return infra.dask.clean_write_parquet(df, out_path, compute=False)


def shift_flows_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        future_handles.append(shift_flows(df_path, df_out_path, 9))

    print("Recursive flow shift now")
    client.compute(future_handles, sync=True)


def shift_dns_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        future_handles.append(shift_dns(df_path, df_out_path, 9))

    print("Recursive dns shift now")
    client.compute(future_handles, sync=True)


def shift_flows_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df["start"] = df["start"] + pd.tseries.offsets.DateOffset(hours=9)
    df["end"] = df["end"] + pd.tseries.offsets.DateOffset(hours=9)
    print("Single layer flow shift now")
    infra.dask.clean_write_parquet(df, out_path)


def shift_dns_flat(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.reset_index()
    df["timestamp"] = df["timestamp"] + pd.tseries.offsets.DateOffset(hours=9)
    print("Single layer dns shift now")
    df = df.set_index("timestamp")
    infra.dask.clean_write_parquet(df, out_path)


def shift_transactions_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df["timestamp"] = df["timestamp"] + pd.tseries.offsets.DateOffset(hours=9)
    print("Single layer timestamp shift now")
    infra.dask.clean_write_parquet(df, out_path)


if __name__ == "__main__":
    client = infra.dask.setup_dask_client()
    shift_flows_recursive("data/clean/flows/typical_fqdn_DIV_user_INDEX_start",
                          "data/clean/flows/typical_fqdn_TZ_DIV_user_INDEX_start",
                          client)

    shift_flows_flat_noindex("data/clean/flows/typical_DIV_none_INDEX_user",
                             "data/clean/flows/typical_TZ_DIV_none_INDEX_user")

    shift_flows_flat_noindex("data/clean/flows/nouser_DIV_none_INDEX_none",
                             "data/clean/flows/nouser_TZ_DIV_none_INDEX_none")

    shift_flows_flat_noindex("data/clean/flows/p2p_DIV_none_INDEX_none",
                             "data/clean/flows/p2p_TZ_DIV_none_INDEX_none")

    shift_dns_recursive("data/clean/dns/successful_DIV_user_INDEX_timestamp",
                        "data/clean/dns/successful_TZ_DIV_user_INDEX_timestamp",
                        client)

    shift_dns_flat("data/clean/dns/successful_DIV_none_INDEX_timestamp",
                   "data/clean/dns/successful_TZ_DIV_none_INDEX_timestamp")

    shift_transactions_flat_noindex("data/clean/transactions",
                                    "data/clean/transactions_TZ")

