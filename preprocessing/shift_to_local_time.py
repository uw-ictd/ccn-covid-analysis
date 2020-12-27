import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def shift_flows(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    df = df.reset_index()

    df["start"] = df["start"] + pd.Timedelta(
        hours=infra.constants.LOCAL_TIME_UTC_OFFSET_HOURS)
    df["end"] = df["end"] + pd.Timedelta(
        hours=infra.constants.LOCAL_TIME_UTC_OFFSET_HOURS)

    df = df.set_index("start")

    return infra.dask.clean_write_parquet(df, out_path, compute=False)


def shift_transactions_flat_noindex(in_path, out_path):
    df = infra.pd.read_parquet(in_path)
    df["timestamp"] = df["timestamp"] + pd.Timedelta(
        hours=infra.constants.LOCAL_TIME_UTC_OFFSET_HOURS)
    print("Single layer timestamp shift now")
    infra.pd.clean_write_parquet(df, out_path)


def shift_all(client):
    future_handles = [
        shift_flows(
            "scratch/flows/typical_fqdn_org_category_local_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TZ_DIV_none_INDEX_start",
        ),
        shift_flows(
            "scratch/flows/p2p_DIV_none_INDEX_start",
            "scratch/flows/p2p_TZ_DIV_none_INDEX_start",
        ),
        shift_flows(
            "scratch/flows/nouser_DIV_none_INDEX_start",
            "scratch/flows/nouser_TZ_DIV_none_INDEX_start",
        )
    ]

    client.compute(future_handles, sync=True)

    shift_transactions_flat_noindex("scratch/transactions.parquet",
                                    "scratch/transactions_TZ.parquet")


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(20, platform)

    shift_all(client=dask_client)
