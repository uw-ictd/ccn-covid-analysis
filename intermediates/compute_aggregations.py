from pathlib import Path
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def compute_all_intermediate_aggregations(outpath, dask_client):
    compute_bytes_per_category_per_user_per_day(Path(outpath)/"bytes_per_category_per_user_per_day.parquet")
    compute_bytes_per_org_per_user_per_day(outpath/"bytes_per_org_per_user_per_day.parquet")
    compute_bytes_per_user_per_domain_per_day(outpath/"bytes_per_user_per_domain_per_day.parquet")


def compute_bytes_per_category_per_user_per_day(outfile):
    flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start")[["user", "category", "bytes_up", "bytes_down"]]

    flows = flows.reset_index()
    flows["day"] = flows["start"].dt.floor("d")
    flows = flows[["user", "category", "day", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "category", "day"]).sum()
    flows = flows.compute()
    print(flows)

    infra.pd.clean_write_parquet(flows, outfile)


def compute_bytes_per_org_per_user_per_day(outfile):
    flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start")[["user", "org", "bytes_up", "bytes_down"]]

    flows = flows.reset_index()
    flows["day"] = flows["start"].dt.floor("d")
    flows = flows[["user", "org", "day", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "org", "day"]).sum()
    flows = flows.compute()
    print(flows)

    infra.pd.clean_write_parquet(flows, outfile)


def compute_bytes_per_user_per_domain_per_day(outfile):
    flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start")[["fqdn", "user", "bytes_up", "bytes_down"]]

    flows = flows.astype({
        "user": object,
        "fqdn": object
    })

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "fqdn", "user"]).sum()
    flows = flows.compute()

    flows = flows.reset_index()
    infra.pd.clean_write_parquet(flows, outfile)


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    aggregate_location = Path("data/aggregates")
    aggregate_location.mkdir(parents=True, exist_ok=True)

    if platform.large_compute_support:
        print("Running compute tasks")
        print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
        client = infra.dask.setup_platform_tuned_dask_client(40, platform, single_threaded_workers=True)
        compute_all_intermediate_aggregations(outpath=aggregate_location, dask_client=client)
        client.close()
    else:
        print("Skipping heavy compute, no work done!")

    print("Done!")