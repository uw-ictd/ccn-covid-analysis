import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def anonymize_rare_orgs(in_path, out_path):
    flows = infra.dask.read_parquet(in_path)
    user_counts_per_org = flows[["user", "org", "bytes_up"]].groupby(["user", "org"]).first().dropna().reset_index()
    user_counts_per_org = user_counts_per_org.assign(user_count=1).groupby(["org"]).sum()
    user_counts_per_org = user_counts_per_org.drop("bytes_up", axis=1)
    flows_with_counts = flows.merge(user_counts_per_org, on="org")
    flows_with_counts = flows_with_counts.astype({
        "org": object,
    })
    flows_with_counts["org"] = flows_with_counts["org"].mask(
        flows_with_counts["user_count"] < infra.constants.MIN_K_ANON,
        other="Other [Anonymized U<5]",
        )
    flows_with_counts = flows_with_counts.categorize(columns=["fqdn_source", "org", "category"])
    flows = flows_with_counts.drop("user_count", axis=1)
    infra.dask.clean_write_parquet(flows, out_path)


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

        anonymize_rare_orgs(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON",
        )

        client.close()

    print("Done!")
