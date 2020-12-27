import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def optimize_typical_flow_frame(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    print(df)
    print(df.index)
    df = df.categorize(columns=["fqdn_source", "org", "category", "user", "dest_ip"])
    df = df.astype({
        "user_port": int,
        "dest_port": int,
        "bytes_up": int,
        "bytes_down": int,
        "protocol": int,
        "ambiguous_fqdn_count": int
        })
    print(df)
    df = df.repartition(partition_size="128M", force=True)
    df = df.reset_index().set_index("start")
    print(df)

    infra.dask.clean_write_parquet(df, out_path)


def optimize_all(client):
    optimize_typical_flow_frame(
        "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org_fqdn_ip",
    "scratch/flows/typical_OPT_INDEX_start")


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(20, platform)

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    optimize_all(dask_client)

    dask_client.close()

    print("Done!")
