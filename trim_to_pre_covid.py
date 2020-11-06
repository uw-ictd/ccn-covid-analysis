import pandas as pd

import infra.constants
import infra.dask_infra
import infra.pd_infra
import infra.platform

from infra.mapped_ips import IpProcessor


def _explore_unknowns(in_path):
    flows = infra.dask_infra.read_parquet(in_path)
    unknown_flows = flows.loc[(flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")]
    unknown_flows["mbytes_total"] = (unknown_flows["bytes_up"] + unknown_flows["bytes_down"]) / 1000**2
    unknown_flows = unknown_flows.groupby(["dest_ip"]).sum()
    unknown_flows = unknown_flows.compute()
    infra.pd_infra.clean_write_parquet(unknown_flows, "scratch/flows/temp-unkown-fqdns")
    unknown_flows = infra.pd_infra.read_parquet("scratch/flows/temp-unkown-fqdns")
    unknown_flows = unknown_flows.sort_values(["mbytes_total"])
    print(unknown_flows.tail(40))


def annotate_category_org_from_ip(in_path, out_path):
    flows = infra.dask_infra.read_parquet(in_path)
    flows = flows.reset_index()
    flows = flows.astype({
        "org": object,
        "category": object,
    })

    ip_processor = IpProcessor()
    flows["iporg"] = flows.loc[
        (flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")
    ].apply(
        lambda row: ip_processor.process_ip(row["dest_ip"])[0],
        axis=1,
        meta=("iporg", object),
    )

    flows["ipcategory"] = flows.loc[
        (flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")
    ].apply(
        lambda row: ip_processor.process_ip(row["dest_ip"])[1],
        axis=1,
        meta=("ipcategory", object),
    )

    flows["category"] = flows["category"].mask(
        ((flows["category"] == "Unknown (No DNS)") & (flows["ipcategory"] != None)),
        other=flows["ipcategory"]
    )
    flows["org"] = flows["org"].mask(
        ((flows["org"] == "Unknown (No DNS)") & (flows["iporg"] != None)),
        other=flows["iporg"]
    )

    flows = flows.drop(["ipcategory", "iporg"], axis=1)
    # flows = flows.categorize(columns=["fqdn_source", "org", "category"])
    infra.dask_infra.clean_write_parquet(flows, out_path)


def anonymize_flows(in_path, out_path):
    flows = infra.dask_infra.read_parquet(in_path)
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
    infra.dask_infra.clean_write_parquet(flows, out_path)


def trim_flows_flat_noindex(in_path, out_path):
    df = infra.dask_infra.read_parquet(in_path)
    df = df.loc[(df["start"] >= infra.constants.MIN_DATE) & (df["start"] < infra.constants.MAX_DATE)]
    df = df.set_index("start").repartition(partition_size="128M")
    infra.dask_infra.clean_write_parquet(df, out_path)


def trim_flows_flat_indexed(in_path, out_path):
    df = infra.dask_infra.read_parquet(in_path)
    df = df.loc[(df.index >= infra.constants.MIN_DATE) & (df.index < infra.constants.MAX_DATE)]
    infra.dask_infra.clean_write_parquet(df, out_path)


def trim_transactions_flat_noindex(in_path, out_path):
    df = infra.dask_infra.read_parquet(in_path)
    df = df.loc[(df["timestamp"] >= infra.constants.MIN_DATE) & (df["timestamp"] < infra.constants.MAX_DATE)]
    infra.pd_infra.clean_write_parquet(df.compute(), out_path)


def trim_log_gaps_flat_noindex(in_path, out_path):
    df = infra.pd_infra.read_parquet(in_path)
    df = df.loc[(df["start"] >= infra.constants.MIN_DATE) & (df["start"] < infra.constants.MAX_DATE)]
    infra.pd_infra.clean_write_parquet(df, out_path)


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
        client = infra.dask_infra.setup_platform_tuned_dask_client(5, platform)

        _explore_unknowns("data/internal/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
        annotate_category_org_from_ip(
            "data/internal/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
        )
        anonymize_flows(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON",
        )
        trim_flows_flat_noindex(
            "scratch/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start_ANON",
            "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start",
        )

        trim_flows_flat_indexed(
            "data/internal/flows/p2p_TM_DIV_none_INDEX_start",
            "data/clean/flows/p2p_TM_DIV_none_INDEX_start",
        )

        trim_flows_flat_indexed(
            "data/internal/flows/nouser_TM_DIV_none_INDEX_start",
            "data/clean/flows/nouser_TM_DIV_none_INDEX_start",
        )

        trim_transactions_flat_noindex(
            "data/internal/transactions_TM",
            "data/clean/transactions_TM.parquet",
        )

        trim_log_gaps_flat_noindex(
            "data/internal/log_gaps_TM.parquet",
            "data/clean/log_gaps_TM.parquet",
        )

        client.close()

    print("Done!")
