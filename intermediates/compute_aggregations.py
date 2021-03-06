from pathlib import Path
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def compute_all_intermediate_aggregations(outpath, dask_client):
    flows = infra.dask.read_parquet("data/clean/flows_typical_DIV_none_INDEX_start")
    flows = flows.reset_index()
    flows["day"] = flows["start"].dt.floor("d")
    flows = flows.astype({
        "user": object,
        "fqdn": object,
        "org": object,
        "category": object,
    })
    flows = dask_client.persist(flows)

    compute_bytes_per_category_per_user_per_day(
        flows,
        outpath/"bytes_per_category_per_user_per_day.parquet"
    )
    print("Completed category per user per day")

    compute_bytes_per_org_per_user_per_day(
        flows,
        outpath/"bytes_per_org_per_user_per_day.parquet"
    )
    print("Completed org per user per day")

    compute_bytes_per_user_per_domain_per_day(
        flows,
        outpath/"bytes_per_user_per_domain_per_day.parquet"
    )
    print("Completed fqdn per user per day")


def compute_bytes_per_category_per_user_per_day(flows, outfile):
    flows = flows[["user", "category", "day", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "category", "day"]).sum().compute()

    flows = flows.reset_index()
    infra.pd.clean_write_parquet(flows, outfile)


def compute_bytes_per_org_per_user_per_day(flows, outfile):
    flows = flows[["user", "org", "day", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "org", "day"]).sum().compute()

    flows = flows.reset_index()
    infra.pd.clean_write_parquet(flows, outfile)


def compute_bytes_per_user_per_domain_per_day(flows, outfile):
    flows = flows[["user", "fqdn", "day", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "fqdn", "day"]).sum().compute()

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