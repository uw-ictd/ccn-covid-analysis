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
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["bytes_up", "bytes_down"]]
    p_to_p = bok.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")[["bytes_b_to_a", "bytes_a_to_b"]]

    typical_flow_count = typical.shape[0]
    p2p_flow_count = p_to_p.shape[0]

    typical_downlink_bytes = typical["bytes_down"].sum()
    typical_uplink_bytes = typical["bytes_up"].sum()
    p2p_bytes = (p_to_p["bytes_b_to_a"] + p_to_p["bytes_a_to_b"]).sum()

    (typical_flow_count, p2p_flow_count, typical_downlink_bytes, typical_uplink_bytes, p2p_bytes) = client.compute(
        [typical_flow_count, p2p_flow_count, typical_downlink_bytes, typical_uplink_bytes, p2p_bytes], sync=True)

    print("Typical Flow Count:", typical_flow_count)
    print("P2P Flow Count:", p2p_flow_count)
    print("Total Flow Count:", p2p_flow_count + typical_flow_count)

    print("Total typical DL Gbytes:", typical_downlink_bytes/1000**3)
    print("Total typical UL Gbytes:", typical_uplink_bytes/1000**3)
    print("Total p2p Gbytes:", p2p_bytes/1000**3)
    print("Total GBytes:", (typical_downlink_bytes + typical_uplink_bytes + p2p_bytes)/1000**3)

    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM")
    print("Total transactions:", len(transactions))


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

        client.close()

    print("Done!")
