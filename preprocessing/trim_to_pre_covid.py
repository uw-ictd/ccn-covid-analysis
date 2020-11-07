import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def trim_flows_flat_noindex(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    df = df.loc[(df["start"] >= infra.constants.MIN_DATE) & (df["start"] < infra.constants.MAX_DATE)]
    df = df.set_index("start").repartition(partition_size="128M")
    infra.dask.clean_write_parquet(df, out_path)


def trim_flows_flat_indexed(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    df = df.loc[(df.index >= infra.constants.MIN_DATE) & (df.index < infra.constants.MAX_DATE)]
    infra.dask.clean_write_parquet(df, out_path)


def trim_transactions_flat_noindex(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    df = df.loc[(df["timestamp"] >= infra.constants.MIN_DATE) & (df["timestamp"] < infra.constants.MAX_DATE)]
    infra.pd.clean_write_parquet(df.compute(), out_path)


def trim_log_gaps_flat_noindex(in_path, out_path):
    df = infra.pd.read_parquet(in_path)
    df = df.loc[(df["start"] >= infra.constants.MIN_DATE) & (df["start"] < infra.constants.MAX_DATE)]
    infra.pd.clean_write_parquet(df, out_path)


if __name__ == "__main__":
    platform = infra.platform.read_config()

    print("Min:", infra.constants.MIN_DATE)
    print("Max:", infra.constants.MAX_DATE)

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    if platform.large_compute_support:
        print("Running compute tasks")
        client = infra.dask.setup_platform_tuned_dask_client(5, platform)

        trim_flows_flat_noindex(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON",
            "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
        )

        trim_flows_flat_indexed(
            "../data/internal/flows/p2p_TM_DIV_none_INDEX_start",
            "data/clean/flows/p2p_TM_DIV_none_INDEX_start",
        )

        trim_flows_flat_indexed(
            "../data/internal/flows/nouser_TM_DIV_none_INDEX_start",
            "data/clean/flows/nouser_TM_DIV_none_INDEX_start",
        )

        trim_transactions_flat_noindex(
            "../data/internal/transactions_TM",
            "data/clean/transactions_TM.parquet",
        )

        trim_log_gaps_flat_noindex(
            "../data/internal/log_gaps_TM.parquet",
            "data/clean/log_gaps_TM.parquet",
        )

        client.close()

    print("Done!")
