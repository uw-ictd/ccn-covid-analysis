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
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
        )
    flows_with_counts = flows_with_counts.categorize(columns=["fqdn_source", "org", "category"])
    flows = flows_with_counts.drop("user_count", axis=1)
    infra.dask.clean_write_parquet(flows, out_path)


def anonymize_rare_fqdns(in_path, out_path):
    flows = infra.dask.read_parquet(in_path)
    user_counts_per_org = flows[["user", "fqdn", "bytes_up"]].groupby(["user", "fqdn"]).first().dropna().reset_index()
    user_counts_per_org = user_counts_per_org.assign(user_count=1).groupby(["fqdn"]).sum()
    user_counts_per_org = user_counts_per_org.drop("bytes_up", axis=1)
    flows_with_counts = flows.merge(user_counts_per_org, on="fqdn")
    flows_with_counts = flows_with_counts.astype({
        "fqdn": object,
    })
    flows_with_counts["fqdn"] = flows_with_counts["fqdn"].mask(
        flows_with_counts["user_count"] < infra.constants.MIN_K_ANON,
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
        )
    flows_with_counts = flows_with_counts.categorize(columns=["fqdn_source", "fqdn", "category"])
    flows = flows_with_counts.drop("user_count", axis=1)
    infra.dask.clean_write_parquet(flows, out_path)


def anonymize_rare_ips(in_path, out_path):
    flows = infra.dask.read_parquet(in_path)
    user_counts_per_org = flows[["user", "dest_ip", "bytes_up"]].groupby(["user", "dest_ip"]).first().dropna().reset_index()
    user_counts_per_org = user_counts_per_org.assign(user_count=1).groupby(["dest_ip"]).sum()
    user_counts_per_org = user_counts_per_org.drop("bytes_up", axis=1)
    flows_with_counts = flows.merge(user_counts_per_org, on="dest_ip")
    flows_with_counts = flows_with_counts.astype({
        "dest_ip": object,
    })
    flows_with_counts["dest_ip"] = flows_with_counts["dest_ip"].mask(
        flows_with_counts["user_count"] < infra.constants.MIN_K_ANON,
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
        )
    flows = flows_with_counts.drop("user_count", axis=1)
    infra.dask.clean_write_parquet(flows, out_path)


def optimize_flow_frame(in_path, out_path):
    pass
    # flows_with_counts = flows_with_counts.categorize(columns=["fqdn_source", "dest_ip", "category"])


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    if platform.large_compute_support:
        print("Running compute tasks")
        client = infra.dask.setup_platform_tuned_dask_client(20, platform)

        anonymize_rare_orgs(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org",
        )
        anonymize_rare_fqdns(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org_fqdn",
        )
        anonymize_rare_ips(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org_fqdn",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON_org_fqdn_ip",
        )

        client.close()

    print("Done!")
