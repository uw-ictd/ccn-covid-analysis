""" Bytes per organization and category by user quantile
"""

import altair as alt
import pandas as pd

import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    flows = infra.dask.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "category", "org", "bytes_up", "bytes_down"]]

    # Do the grouping
    flows = flows.groupby(["user", "category", "org"]).sum()
    flows = flows.compute()

    infra.pd.clean_write_parquet(flows, outfile)


def make_category_quantiles_plots(infile):
    grouped_flows = infra.pd.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]
    user_category_total = grouped_flows[["user", "category", "bytes_total"]].groupby(
        ["user", "category"]
    ).sum().reset_index()

    # Filter users by time in network to eliminate early incomplete samples
    user_active_ranges = infra.pd.read_parquet(
        "data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active", "days_online"]]
    # Drop users that joined less than a week ago.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] > 7
        ]
    # Drop users active for less than one day
    users_to_analyze = users_to_analyze.loc[
        users_to_analyze["days_active"] > 1,
    ]

    # Sort categories by total amount of bytes.
    cat_totals = grouped_flows.groupby("category").sum().reset_index()
    cat_sort_order = cat_totals.sort_values("bytes_total", ascending=False).set_index("bytes_total").reset_index()
    cat_sort_order["rank"] = cat_sort_order["bytes_total"].rank(method="min", ascending=False)
    cat_sort_list = cat_sort_order["category"].tolist()

    # Group users by quantiles of their daily use.
    user_totals = user_category_total.groupby("user").sum().reset_index()
    user_totals = user_totals.merge(users_to_analyze, on="user", how="inner")
    user_totals["avg_daily_bytes"] = user_totals["bytes_total"] / user_totals["days_online"]
    user_totals["rank_total"] = user_totals["bytes_total"].rank(method="min", pct=True)

    user_totals["rank_daily"] = user_totals["avg_daily_bytes"].rank(method="min")
    user_totals["quantile"] = pd.cut(user_totals["rank_daily"], 10, precision=0, right=False, include_lowest=True)

    # Compute the share of each user's traffic in each category
    user_shares = user_totals.rename(columns={"bytes_total": "user_bytes_total"})
    user_shares = user_category_total.merge(user_shares[["user", "user_bytes_total"]], on="user", how="inner")
    user_shares["category_share"] = user_shares["bytes_total"] / user_shares["user_bytes_total"]
    user_shares = user_shares[["user", "category",  "category_share"]]

    # Merge the user quantile information back into the flows, and then group by category
    quantile_flows = user_category_total.merge(user_totals[["user", "quantile", "days_online"]], on="user", how="inner")
    quantile_flows["normalized_bytes_total"] = quantile_flows["bytes_total"] / quantile_flows["days_online"]

    # Merge category share information into the plot frame
    quantile_flows = quantile_flows.merge(user_shares, on=["user", "category"], how="inner")

    # Compute means for quantiles and quantile labels
    quantile_totals = quantile_flows.groupby(["quantile", "category"]).mean()
    quantile_totals = quantile_totals.reset_index()
    quantile_totals["quantile_str"] = quantile_totals["quantile"].apply(lambda x: str(x))

    # Add sort information back to rendered dataframe
    quantile_totals = quantile_totals.merge(cat_sort_order[["category", "rank"]], on="category", how="inner")

    # This might not be showing exactly what I want to show, since in merging
    # users some users that dominate video could be overrepresented. Maybe
    # want to merge on the fraction of traffic to each part from each user?
    # Are users counted equally or are bytes counted equally...
    alt.Chart(quantile_totals[["category", "quantile_str", "bytes_total", "rank", "normalized_bytes_total"]]).mark_bar().encode(
        x="quantile_str:O",
        y=alt.Y(
            "normalized_bytes_total",
            stack="normalize",
            sort=cat_sort_list,
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_per_quantile_bar.png",
        scale_factor=2,
    )

    quantile_totals["normalize_mb_total"] = quantile_totals["normalized_bytes_total"] / 1000.0**2

    # Generate an order based on the intervals, not the strings, to correctly sort the axis.
    quantiles = quantile_totals[["quantile", "quantile_str"]].groupby(["quantile"]).first()
    quantiles = quantiles["quantile_str"].to_list()
    alt.Chart(
        quantile_totals[["category", "quantile_str", "bytes_total", "rank", "normalize_mb_total"]]
    ).mark_line().encode(
        x=alt.X(
            "quantile_str:N",
            title="User by Rank of Average Use Per Online Day (Grouped)",
            sort=quantiles,
        ),
        y=alt.Y(
            "normalize_mb_total",
            sort=cat_sort_list,
            title="Average Traffic Per Online Day (MB)"
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
            legend=alt.Legend(
                title="Category",
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
                columns=2,
                symbolLimit=20,
            ),
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).configure_axisX(
        labelAngle=0,
        labelFontSize=7,
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_per_quantile_line.png",
        scale_factor=2,
    )

    alt.Chart(
        quantile_totals[["category", "quantile_str", "category_share", "rank"]]
    ).mark_line().encode(
        x=alt.X(
            "quantile_str:N",
            title="User by Rank of Average Use Per Online Day (Grouped)",
            sort=quantiles,
        ),
        y=alt.Y(
            "category_share",
            sort=cat_sort_list,
            title="Average Fraction of Traffic Per User"
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=cat_sort_list,
            legend=alt.Legend(
                title="Category",
                orient="none",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
                columns=4,
                labelFontSize=8,
                legendX=10,
                legendY=3,
                symbolLimit=20,
            ),
        ),
        order=alt.Order(
            "rank",
            sort="descending",
        ),
    ).configure_axisX(
        labelAngle=0,
        labelFontSize=7,
    ).properties(
        width=500,
    ).save(
        "renders/bytes_per_category_share_per_quantile_line.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()
    graph_temporary_file = "scratch/graphs/bytes_per_category_per_quantile"

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    if platform.large_compute_support:
        print("Running compute tasks")
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        pd.set_option('display.max_columns', None)
        make_category_quantiles_plots(graph_temporary_file)

    print("Done!")
