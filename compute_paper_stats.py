import pandas as pd

import bok.constants
import bok.dask_infra
import bok.pd_infra
import bok.platform


def _explore_unknowns(in_path):
    flows = bok.dask_infra.read_parquet(in_path)
    unknown_flows = flows.loc[(flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")]
    unknown_flows["mbytes_total"] = (unknown_flows["bytes_up"] + unknown_flows["bytes_down"]) / 1000**2
    unknown_flows = unknown_flows.groupby(["dest_ip"]).sum()
    unknown_flows = unknown_flows.compute()
    bok.pd_infra.clean_write_parquet(unknown_flows, "scratch/flows/temp-unkown-fqdns")
    unknown_flows = bok.pd_infra.read_parquet("scratch/flows/temp-unkown-fqdns")
    unknown_flows = unknown_flows.sort_values(["mbytes_total"])
    print(unknown_flows.tail(40))


def _compute_counts(dask_client):
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["bytes_up", "bytes_down", "local"]]
    p_to_p = bok.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")[["bytes_b_to_a", "bytes_a_to_b"]]

    typical_flow_count = typical.shape[0]
    p2p_flow_count = p_to_p.shape[0]

    internet_flows = typical.loc[typical["local"] == False]

    internet_flow_count = internet_flows.shape[0]
    intranet_flow_count = typical_flow_count - internet_flow_count + p2p_flow_count

    internet_downlink_bytes = internet_flows["bytes_down"].sum()
    internet_uplink_bytes = internet_flows["bytes_up"].sum()
    p2p_bytes = (p_to_p["bytes_b_to_a"] + p_to_p["bytes_a_to_b"]).sum()

    local_bytes = (typical["bytes_down"] + typical["bytes_up"]).sum() - internet_downlink_bytes - internet_uplink_bytes
    intranet_bytes = local_bytes + p2p_bytes

    (internet_flow_count, intranet_flow_count, internet_downlink_bytes, internet_uplink_bytes, p2p_bytes, intranet_bytes) = dask_client.compute(
        [internet_flow_count, intranet_flow_count, internet_downlink_bytes, internet_uplink_bytes, p2p_bytes, intranet_bytes],
        sync=True)

    print("Internet Flow Count:", internet_flow_count)
    print("intranet Flow Count:", intranet_flow_count)
    print("Total Flow Count:", internet_flow_count + intranet_flow_count)

    print("Total internet DL Gbytes:", internet_downlink_bytes/1000**3)
    print("Total internet UL Gbytes:", internet_uplink_bytes/1000**3)
    print("Total internet Gbytes:", (internet_uplink_bytes + internet_downlink_bytes)/1000**3)
    print("Total intranet Gbytes:", intranet_bytes/1000**3)
    print("Total GBytes:", (internet_uplink_bytes + internet_downlink_bytes + intranet_bytes)/1000**3)

    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM")
    print("Purchase transactions:", len(transactions.loc[transactions["kind"] == "purchase"]))
    print("Total transactions:", len(transactions))


def _compute_dns_percentages(dask_client):
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")

    print(typical)


def _compute_dates():
    print("Included Dates")
    print("Start:", bok.constants.MIN_DATE)
    print("End:", bok.constants.MAX_DATE)
    print("Length:", bok.constants.MAX_DATE - bok.constants.MIN_DATE)


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        _compute_counts(client)
        _compute_dns_percentages(client)

        client.close()

    _compute_dates()

    print("Done!")
