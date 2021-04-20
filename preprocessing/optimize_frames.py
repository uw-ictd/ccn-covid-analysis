import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def optimize_typical_flow_frame(in_path, out_path, client):
    df = infra.dask.read_parquet(in_path)
    df = df.categorize(columns=["fqdn_source", "org", "category", "user", "dest_ip"])
    df = df.astype({
        "user_port": int,
        "dest_port": int,
        "bytes_up": int,
        "bytes_down": int,
        "protocol": int,
        "ambiguous_fqdn_count": int
        })

    df = df.set_index("start")
    persisted_df = client.persist(df)

    persisted_df = persisted_df.repartition(partition_size="128M", force=True)

    infra.dask.clean_write_parquet(persisted_df, out_path)
    client.cancel(persisted_df)
    del persisted_df


def optimize_p2p_flow_frame(in_path, out_path, client):
    df = infra.dask.read_parquet(in_path)
    df = df.categorize(columns=["user_a", "user_b"])
    persisted_df = client.persist(df)
    persisted_df = persisted_df.repartition(partition_size="128M", force=True)

    infra.dask.clean_write_parquet(persisted_df, out_path)
    client.cancel(persisted_df)
    del persisted_df


def optimize_nouser_flow_frame(in_path, out_path, client):
    df = infra.dask.read_parquet(in_path)
    df = df.categorize(columns=["ip_a", "ip_b"])
    persisted_df = client.persist(df)
    persisted_df = persisted_df.repartition(partition_size="128M", force=True)

    infra.dask.clean_write_parquet(persisted_df, out_path)
    client.cancel(persisted_df)
    del persisted_df


def optimize_transactions_frame(in_path, out_path):
    df = infra.pd.read_parquet(in_path)
    df["amount_bytes"] = df["amount_bytes"].fillna(0)
    df["user"] = df["user"].fillna("[None]")
    df["dest_user"] = df["dest_user"].fillna("[None]")
    df = df.astype({
        "amount_bytes": int,
        "user": "category",
        "dest_user": "category",
    })
    df = df.set_index("timestamp")
    infra.pd.clean_write_parquet(df, out_path)


def optimize_all(client):
    optimize_transactions_frame(
        "scratch/transactions_TM.parquet",
        "scratch/transactions_OPT_DIV_none_INDEX_timestamp.parquet",
    )

    optimize_p2p_flow_frame(
        "scratch/flows/p2p_TM_DIV_none_INDEX_start",
        "scratch/flows/p2p_OPT_DIV_none_INDEX_start",
        client,
    )

    optimize_nouser_flow_frame(
        "scratch/flows/nouser_TM_DIV_none_INDEX_start",
        "scratch/flows/nouser_OPT_DIV_none_INDEX_start",
        client,
    )

    optimize_typical_flow_frame(
        "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_none_ANON_org_fqdn_ip",
        "scratch/flows/typical_OPT_DIV_none_INDEX_start",
        client,
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(20, platform)

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 20)

    optimize_all(dask_client)

    dask_client.close()

    print("Done!")
