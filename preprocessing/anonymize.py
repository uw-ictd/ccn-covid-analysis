import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def anonymize_rare_orgs(client, flows, checkpoint_path=None):
    print("Anonymizing orgs")
    user_counts_per_org = flows[["user", "org", "bytes_up"]].groupby(["user", "org"]).first().dropna().reset_index()
    user_counts_per_org = user_counts_per_org.assign(user_count=1).groupby(["org"]).sum()
    user_counts_per_org = user_counts_per_org.drop("bytes_up", axis=1).reset_index()
    flows_with_counts = flows.merge(user_counts_per_org, on="org", how="left")

    flows_with_counts["org"] = flows_with_counts["org"].mask(
        (flows_with_counts["user_count"] < infra.constants.MIN_K_ANON) | flows_with_counts["user_count"].isna(),
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
    )

    flows = flows_with_counts.drop("user_count", axis=1)

    flows = client.persist(flows)
    if checkpoint_path is not None:
        infra.dask.clean_write_parquet(flows, checkpoint_path)

    return flows


def anonymize_rare_ips(client, flows, checkpoint_path=None):
    print("Anonymizing destination ip addresses")
    user_counts_per_ip = flows[["user", "dest_ip", "bytes_up"]].groupby(["user", "dest_ip"]).first().dropna().reset_index()
    user_counts_per_ip = user_counts_per_ip.assign(user_count=1).groupby(["dest_ip"]).sum()
    user_counts_per_ip = user_counts_per_ip.drop("bytes_up", axis=1).reset_index()
    flows_with_counts = flows.merge(user_counts_per_ip, on="dest_ip", how="left")

    flows_with_counts["dest_ip"] = flows_with_counts["dest_ip"].mask(
        (flows_with_counts["user_count"] < infra.constants.MIN_K_ANON) | flows_with_counts["user_count"].isna(),
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
        )
    flows = flows_with_counts.drop("user_count", axis=1)

    flows = client.persist(flows)
    if checkpoint_path is not None:
        infra.dask.clean_write_parquet(flows, checkpoint_path)

    return flows


def anonymize_rare_fqdns(client, flows, checkpoint_path=None):
    print("Anonymizing fqdns")
    user_counts_per_fqdn = flows[["user", "fqdn", "bytes_up"]].groupby(["user", "fqdn"]).first().dropna().reset_index()
    user_counts_per_fqdn = user_counts_per_fqdn.assign(user_count=1).groupby(["fqdn"]).sum()
    user_counts_per_fqdn = user_counts_per_fqdn.drop("bytes_up", axis=1).reset_index()
    flows_with_counts = flows.merge(user_counts_per_fqdn, on="fqdn", how="left")

    flows_with_counts["fqdn"] = flows_with_counts["fqdn"].mask(
        (flows_with_counts["user_count"] < infra.constants.MIN_K_ANON) | flows_with_counts["user_count"].isna(),
        other="[Other Anonymized U<{}]".format(infra.constants.MIN_K_ANON),
        )
    flows = flows_with_counts.drop("user_count", axis=1)

    flows = client.persist(flows)
    if checkpoint_path is not None:
        infra.dask.clean_write_parquet(flows, checkpoint_path)

    return flows


def anonymize_all(client):
    df = infra.dask.read_parquet("scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    # Reset the index immediately to preserve the start column across
    # groupings. Don't reindex until after all the anonymization aggregates
    # and groups are complete to avoid unnecessary computation.
    df = df.reset_index()
    df = df.astype({
        "user": object,
        "fqdn": object,
        "org": object,
        "dest_ip": object,
    })

    df_org = anonymize_rare_orgs(
        client,
        df,
        # checkpoint_path="scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_none_ANON_org",
    )
    del df

    df_org_ip = anonymize_rare_ips(
        client,
        df_org,
        checkpoint_path="scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_none_ANON_org_ip",
    )
    del df_org

    final_df = anonymize_rare_fqdns(
        client,
        df_org_ip,
        checkpoint_path="scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_none_ANON_org_fqdn_ip",
    )
    del df_org_ip

    infra.dask.clean_write_parquet(final_df, "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_none_ANON_org_fqdn_ip")


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(20, platform)

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    anonymize_all(dask_client)

    dask_client.close()
    print("Done!")
