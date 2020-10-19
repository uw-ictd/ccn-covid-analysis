import numpy as np
import pandas as pd

import bok.constants
import bok.dask_infra
import bok.pd_infra
import bok.platform

TOTAL_GBYTES = 1339.916865913
TOTAL_INTERNET_GBYTES = 1324.865370317


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
    print("---Raw Counts---")
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

    print("Internet Flow Count:", internet_flow_count, internet_flow_count/(internet_flow_count + intranet_flow_count))
    print("intranet Flow Count:", intranet_flow_count, intranet_flow_count/(internet_flow_count + intranet_flow_count))
    print("Total Flow Count:", internet_flow_count + intranet_flow_count)

    total_internet_gbytes = (internet_uplink_bytes + internet_downlink_bytes)/1000**3
    total_gbytes = total_internet_gbytes + (intranet_bytes/1000**3)
    print("Total internet DL Gbytes:", internet_downlink_bytes/1000**3)
    print("Total internet UL Gbytes:", internet_uplink_bytes/1000**3)
    print("Total internet Gbytes:", total_internet_gbytes, total_internet_gbytes/total_gbytes)
    print("Total intranet Gbytes:", intranet_bytes/1000**3, intranet_bytes/1000**3/total_gbytes)
    print("Total GBytes:", (internet_uplink_bytes + internet_downlink_bytes + intranet_bytes)/1000**3)

    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM")
    print("Purchase transactions:", len(transactions.loc[transactions["kind"] == "purchase"]))
    print("Total transactions:", len(transactions))


def _compute_dns_percentages(dask_client):
    print("---DNS---")
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]

    internet_flows = typical.loc[typical["local"] == False]
    total_count = internet_flows.shape[0]
    total_gb = internet_flows["bytes_total"].sum() / 1000**3

    mapped = internet_flows.loc[internet_flows["fqdn"] != ""]
    mapped_count = mapped.shape[0]
    mapped_gb = mapped["bytes_total"].sum() / 1000**3

    unambiguous = internet_flows.loc[(internet_flows["fqdn"] != "") & (internet_flows["ambiguous_fqdn_count"] == 1)]
    unambiguous_count = unambiguous.shape[0]
    unambiguous_gb = unambiguous["bytes_total"].sum() / 1000**3

    user_derived = internet_flows.loc[(internet_flows["fqdn_source"] == "user_dns_log")]
    user_derived_count = user_derived.shape[0]
    user_derived_gb = user_derived["bytes_total"].sum() / 1000**3

    (total_count, total_gb, mapped_count, mapped_gb, unambiguous_count, unambiguous_gb, user_derived_count, user_derived_gb) = dask_client.compute(
        [total_count, total_gb, mapped_count, mapped_gb, unambiguous_count, unambiguous_gb, user_derived_count, user_derived_gb],
        sync=True,
    )

    print("Name", "count:", "  countpct:", "  GB:", "  GBpct:")
    print("mapped", mapped_count, "&", mapped_count * 100/total_count, "&", mapped_gb, "&", mapped_gb*100/total_gb)
    print("unambiguous", unambiguous_count, "&", unambiguous_count * 100/total_count, "&", unambiguous_gb, "&", unambiguous_gb*100/total_gb)
    print("user_derived", user_derived_count, "&", user_derived_count * 100/total_count, "&", user_derived_gb, "&", user_derived_gb*100/total_gb)


def _compute_category_percentages(dask_client):
    print("---Category/Org---")
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]

    internet_flows = typical.loc[typical["local"] == False]
    total_count = internet_flows.shape[0]
    total_gb = internet_flows["bytes_total"].sum() / 1000**3

    mapped = internet_flows.loc[(internet_flows["category"] != "Unknown (No DNS)") & (internet_flows["category"] != "Unknown (Not Mapped)") & (internet_flows["category"] != "Google (No DNS)")]
    mapped_count = mapped.shape[0]
    mapped_gb = mapped["bytes_total"].sum() / 1000**3

    org_mapped = internet_flows.loc[(internet_flows["org"] != "Unknown (No DNS)") & (internet_flows["org"] != "Unknown (Not Mapped)")]
    org_mapped_count = org_mapped.shape[0]
    org_mapped_gb = org_mapped["bytes_total"].sum() / 1000**3

    unique_orgs = internet_flows["org"].unique()
    unique_cats = internet_flows["category"].unique()

    (total_count, total_gb, mapped_count, mapped_gb, org_mapped_count, org_mapped_gb, unique_cats, unique_orgs) = dask_client.compute(
        [total_count, total_gb, mapped_count, mapped_gb, org_mapped_count, org_mapped_gb, unique_cats, unique_orgs],
        sync=True,
    )

    print("Name", "count:", "  countpct:", "  GB:", "  GBpct:")
    print("category mapped", mapped_count, "&", mapped_count * 100/total_count, "&", mapped_gb, "&", mapped_gb*100/total_gb)
    print("org mapped", org_mapped_count, "&", org_mapped_count * 100/total_count, "&", org_mapped_gb, "&", org_mapped_gb*100/total_gb)
    print("Unique cats", len(unique_cats))
    print("Unique orgs", len(unique_orgs))


def _compute_dates():
    print("---Included Dates---")
    print("Start:", bok.constants.MIN_DATE)
    print("End:", bok.constants.MAX_DATE)
    print("Length:", bok.constants.MAX_DATE - bok.constants.MIN_DATE)


def _internet_uplink_downlink_ratio(dask_client):
    print("---DL UL ratio---")
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    internet_flows = typical.loc[typical["local"] == False]
    dl_ul_ratio = internet_flows["bytes_down"].sum() / internet_flows["bytes_up"].sum()
    print("DL/UL ratio:", dl_ul_ratio.compute(), ":1")


def _total_bigco_traffic(dask_client):
    print("---BigCo total bytes ---")
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]

    facebook_flows = typical.loc[(typical["org"] == "Facebook") | (typical["org"] == "Instagram") | (typical["org"] == "WhatsApp")]
    facebook_gbytes = facebook_flows["bytes_total"].sum() / 1000**3

    google_flows = typical.loc[(typical["org"] == "Google")]
    google_gbytes = google_flows["bytes_total"].sum() / 1000**3

    (facebook_gbytes, google_gbytes) = dask_client.compute(
        [facebook_gbytes, google_gbytes],
        sync=True
    )

    print("Facebook++ gbytes", facebook_gbytes, facebook_gbytes/TOTAL_GBYTES, facebook_gbytes/TOTAL_INTERNET_GBYTES)
    print("Google Gbytes", google_gbytes, google_gbytes/TOTAL_GBYTES, google_gbytes/TOTAL_INTERNET_GBYTES)
    print("Bigco total", (google_gbytes + facebook_gbytes)/TOTAL_GBYTES, (google_gbytes+  facebook_gbytes)/TOTAL_INTERNET_GBYTES)


def _median_offline(dask_client):
    print("---Online Ratio---")
    activity = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")

    # Drop users that have been active less than a week.
    activity = activity.loc[
        activity["days_since_first_active"] >= 7,
    ]

    # Drop users active for less than one week
    activity = activity.loc[
        activity["days_active"] >= 1,
    ]

    activity["online_ratio"] = (
        np.minimum(activity["days_online"], activity["days_active"]) /
        (activity["days_active"] - activity["outage_impact_days"])
    )

    number_users_consistently_on = len(activity.loc[activity["online_ratio"] >= 0.95])
    examined_users = len(activity)
    print("Median user online ratio:", activity["online_ratio"].median())
    print("Number of users consistently on:", number_users_consistently_on)
    print("Fraction of users consistently on:", number_users_consistently_on / examined_users)


def _total_video_traffic(dask_client):
    print("---video total bytes ---")
    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]

    adult_flows = typical.loc[(typical["category"] == "Adult Video")]
    adult_gbytes = adult_flows["bytes_total"].sum() / 1000**3

    video_flows = typical.loc[(typical["category"] == "Video")]
    video_gbytes = video_flows["bytes_total"].sum() / 1000**3

    transactions = bok.pd_infra.read_parquet("data/clean/transactions_TM.parquet")
    purchases = transactions.loc[transactions["kind"] == "purchase"]

    user_ranks = purchases.groupby("user").sum().reset_index()
    user_ranks["rank"] = user_ranks["amount_idr"].rank(method="min", ascending=False)

    users_only_top_20 = user_ranks.loc[user_ranks["rank"] <= 20].copy()
    adult_flows = adult_flows.merge(users_only_top_20[["user", "rank"]], on="user", how="inner")
    video_flows = video_flows.merge(users_only_top_20[["user", "rank"]], on="user", how="inner")
    adult_gbytes_only_top_20 = adult_flows["bytes_total"].sum() / 1000**3
    video_gbytes_only_top_20 = video_flows["bytes_total"].sum() / 1000**3

    top_20_all_flows = typical.merge(users_only_top_20[["user", "rank"]], on="user", how="inner")
    top_20_all_gbytes = top_20_all_flows["bytes_total"].sum() / 1000**3
    top_20_internet_gbytes = top_20_all_flows.loc[top_20_all_flows["local"] == False]["bytes_total"].sum() / 1000 **3

    (adult_gbytes, video_gbytes, adult_gbytes_only_top_20, video_gbytes_only_top_20, top_20_all_gbytes, top_20_internet_gbytes) = dask_client.compute(
        [adult_gbytes, video_gbytes, adult_gbytes_only_top_20, video_gbytes_only_top_20, top_20_all_gbytes, top_20_internet_gbytes],
        sync=True
    )


    print("Adult video gbytes", adult_gbytes, adult_gbytes/TOTAL_GBYTES, adult_gbytes/TOTAL_INTERNET_GBYTES)
    print("General Video Gbytes", video_gbytes, video_gbytes/TOTAL_GBYTES, video_gbytes/TOTAL_INTERNET_GBYTES)
    print("Video (all types) total", (video_gbytes + adult_gbytes)/TOTAL_GBYTES, (video_gbytes+  adult_gbytes)/TOTAL_INTERNET_GBYTES)

    print(
        "Adult video gbytes top 20",
        adult_gbytes_only_top_20,
        adult_gbytes_only_top_20/top_20_all_gbytes,
        adult_gbytes_only_top_20/top_20_internet_gbytes,
    )
    print(
        "General Video Gbytes top 20",
        video_gbytes_only_top_20,
        video_gbytes_only_top_20/top_20_all_gbytes,
        video_gbytes_only_top_20/top_20_internet_gbytes,
    )
    print(
        "Video (all types) total top 20",
        (video_gbytes_only_top_20 + adult_gbytes_only_top_20)/top_20_all_gbytes,
        (video_gbytes_only_top_20 + adult_gbytes_only_top_20)/top_20_internet_gbytes,
    )


def _inequality(client):
    transactions = bok.pd_infra.read_parquet("data/clean/transactions_TM.parquet")
    purchases = transactions.loc[transactions["kind"] == "purchase"]
    purchases["amount_usd"] = purchases["amount_idr"] * bok.constants.IDR_TO_USD

    user_ranks = purchases.groupby("user").sum().reset_index()
    user_ranks["rank"] = user_ranks["amount_idr"].rank(method="min", ascending=False)

    # Find the first day the user was active. Define "active" as making first
    # purchase or first data in network.
    user_active_ranges = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active"]]

    # Drop users that have been active less than a week.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] >= 7,
    ]

    # Drop users active for less than one week
    users_to_analyze = users_to_analyze.loc[
        users_to_analyze["days_active"] >=1,
    ]

    users = user_ranks.merge(users_to_analyze, on="user", how="inner")
    users["spend_rate"] = users["amount_usd"] / users["days_active"]
    print(users.head(40))

    print("Minimum Spend Rate", users["spend_rate"].min())
    print("Max Spend Rate", users["spend_rate"].max())
    print("Mean Spend Rate", users["spend_rate"].mean())
    print("Median Spend Rate", users["spend_rate"].median())

    print("max/mean", users["spend_rate"].max()/users["spend_rate"].mean())
    print("max/median", users["spend_rate"].max()/users["spend_rate"].median())


def _filter_stats(client):
    print("---Filter Stats---")
    activity = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")
    total = len(activity)
    print("Original users:", total)

    # Drop users that have been active less than a week.
    activity = activity.loc[
        activity["days_since_first_active"] >= 7,
    ]

    active_longer_than_week = len(activity)
    print("Joined less than week:", total - active_longer_than_week)

    # Drop users active for less than one week
    activity = activity.loc[
        activity["days_active"] >= 1,
    ]

    final_count = len(activity)
    print("Total active less than one day", active_longer_than_week - final_count)

    print("Final count", final_count, "/", total, "({})%".format(final_count/total))

    typical = bok.dask_infra.read_parquet("data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")
    p2p = bok.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")

    typical = typical.merge(activity, on=["user"], how="inner")
    typical_flow_count = len(typical)
    p2p_flow_count = len(p2p)
    print("Typical", typical_flow_count, "p2p:", p2p_flow_count, "total_flow_count:", typical_flow_count+p2p_flow_count)


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)

    if platform.large_compute_support or True:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        # _compute_counts(client)
        # _compute_dns_percentages(client)
        # _compute_category_percentages(client)
        # _internet_uplink_downlink_ratio(client)
        # _total_bigco_traffic(client)
        # _median_offline(client)
        # _total_video_traffic(client)
        # _inequality(client)
        _filter_stats(client)

        client.close()

    _compute_dates()

    print("Done!")
