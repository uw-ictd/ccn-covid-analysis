import infra.constants
import infra.dask
import infra.pd
import infra.platform


MIN_DATE = infra.constants.MIN_DATE
MAX_DATE = infra.constants.MAX_DATE


def trim_flows(in_path, out_path):
    df = infra.dask.read_parquet(in_path)
    df = df.loc[(df.index >= MIN_DATE) & (df.index < MAX_DATE)]

    return infra.dask.clean_write_parquet(df, out_path, compute=False)


def trim_transactions_flat_noindex(in_path, out_path):
    df = infra.pd.read_parquet(in_path)
    df = df.loc[(df["timestamp"] >= MIN_DATE) & (df["timestamp"] < MAX_DATE)]
    print("Single layer transaction trim  now")
    df["amount_bytes"] = df["amount_bytes"].fillna(value=0)
    df = df.astype({"amount_bytes": int, "amount_idr": int})
    infra.pd.clean_write_parquet(df, out_path)


def trim_all(client):
    future_handles = [
        trim_flows(
            "scratch/flows/typical_fqdn_org_category_local_TZ_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
        ),
        trim_flows(
            "scratch/flows/p2p_TZ_DIV_none_INDEX_start",
            "scratch/flows/p2p_TM_DIV_none_INDEX_start",
        ),
        trim_flows(
            "scratch/flows/nouser_TZ_DIV_none_INDEX_start",
            "scratch/flows/nouser_TM_DIV_none_INDEX_start",
        )
    ]
    client.compute(future_handles, sync=True)

    trim_transactions_flat_noindex(
        "scratch/transactions_TZ.parquet",
        "scratch/transactions_TM.parquet",
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(10, platform)

    trim_all(client=dask_client)

