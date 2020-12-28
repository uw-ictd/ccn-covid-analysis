""" Computes revenue earned by the network by month
"""

import altair
import pandas as pd

import infra.constants
import infra.platform
import infra.pd


def make_expenses():
    maintenance = pd.DataFrame({"timestamp": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="1M")})
    # Monthly power system maintenance
    maintenance = maintenance.assign(amount_idr=1300000)
    maintenance["amount_usd"] = maintenance["amount_idr"] * infra.constants.IDR_TO_USD

    vsat = pd.DataFrame({"timestamp": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="1M")})
    vsat = vsat.assign(amount_usd=300)

    initial = pd.DataFrame({
        "timestamp": [infra.constants.MIN_DATE, infra.constants.OUTAGE_END],
        "amount_usd": [9334, 1000],
    })

    expenses = initial.append(vsat).append(maintenance[["timestamp", "amount_usd"]])
    expenses = expenses.assign(kind="Costs")

    return expenses


def make_plot():
    transactions = infra.pd.read_parquet("data/clean/transactions_DIV_none_INDEX_timestamp.parquet")
    purchases = transactions.loc[(transactions["kind"] == "purchase") | (transactions["kind"] == "admin_topup")]
    purchases = purchases[["timestamp", "amount_idr", "kind", "user"]]

    purchases["amount_usd"] = purchases["amount_idr"] * infra.constants.IDR_TO_USD
    purchases = purchases.loc[purchases["kind"] == "purchase"]

    # Bin by days to limit the number of tuples
    purchases["day"] = purchases["timestamp"].dt.floor("d")
    purchases = purchases.drop("timestamp", axis="columns").rename(columns={"day": "timestamp"})
    purchases = purchases.groupby(["timestamp", "user"]).sum().reset_index()
    purchases = purchases.assign(kind="Total Revenue")

    user_ranks = purchases.groupby("user").sum().reset_index()
    user_ranks["rank"] = user_ranks["amount_usd"].rank(method="min", ascending=False)

    purchases = purchases.merge(user_ranks[["user", "rank"]], on="user", how="inner")

    purchases_no_top_5 = purchases.loc[purchases["rank"] > 5].copy()
    purchases_no_top_5["kind"] = "Revenue Sans Top 5"

    purchases_no_top_10 = purchases.loc[purchases["rank"] > 10].copy()
    purchases_no_top_10["kind"] = "Revenue Sans Top 10"

    purchases_no_top_15 = purchases.loc[purchases["rank"] > 15].copy()
    purchases_no_top_15["kind"] = "Revenue Sans Top 15"

    purchases_no_top_20 = purchases.loc[purchases["rank"] > 20].copy()
    purchases_no_top_20["kind"] = "Revenue Sans Top 20"

    finances = purchases.append(
        make_expenses()
    ).append(
        purchases_no_top_5
    ).append(
        purchases_no_top_10
    ).append(
        purchases_no_top_15
    ).append(
        purchases_no_top_20
    )

    label_order = {
        "Costs": 1,
        "Total Revenue": 2,
        "Revenue Sans Top 5": 3,
        "Revenue Sans Top 10": 4,
        "Revenue Sans Top 15": 5,
        "Revenue Sans Top 20": 6,
    }

    finances = finances.sort_values(["timestamp", "kind"])
    finances = finances.groupby(["timestamp", "kind"]).sum().sort_index()
    finances = finances.reset_index()
    finances = finances.sort_values(["kind"], key=lambda col: col.map(lambda x: label_order[x]))
    finances = finances.sort_values(["timestamp"], kind="mergesort")  # Mergesort is stablely implemented : )
    finances = finances.reset_index()

    finances["amount_cum"] = finances.groupby("kind").cumsum()["amount_usd"]

    altair.Chart(finances).mark_line(interpolate="step-after").encode(
        x=altair.X("timestamp:T",
                   title="Time",
                   ),
        y=altair.Y("amount_cum",
                   title="Amount (USD)",
                   ),
        color=altair.Color(
            "kind",
            title="Cumulative:",
            sort=None,
            legend=altair.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
            ),
        ),
        strokeDash=altair.StrokeDash(
            "kind",
            sort=None,
        )
    ).properties(
        width=500
    ).save(
        "renders/revenue_over_time.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    if platform.large_compute_support:
        print("Running compute tasks")
        pass

    if platform.altair_support:
        print("Running vis tasks")
        make_plot()

    print("Done!")
