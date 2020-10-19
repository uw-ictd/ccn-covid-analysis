"""Compute graphs and statistics of packages purchased and used
"""

import altair as alt

import bok.dask_infra
import bok.pd_infra
import bok.platform


def make_plot():
    transactions = bok.pd_infra.read_parquet("data/clean/transactions_TM.parquet")

    purchases = transactions.loc[transactions["kind"] == "purchase"]
    purchases = purchases.groupby("amount_bytes")["timestamp"].count()
    purchases = purchases.reset_index().rename({"timestamp": "count"}, axis="columns")
    purchases["amount_MB"] = purchases["amount_bytes"] * 1.0/1000**2
    purchases["total_GB"] = purchases["amount_MB"] * purchases["count"] * 1.0/1000

    print(purchases)
    bars = alt.Chart(purchases).mark_bar().encode(
        x=alt.X(
            'count',
            title="Count",
        ),
        y=alt.Y(
            'amount_MB',
            type="ordinal",
            title="Package Type (MB)",
        ),
        color=alt.Color(
            'amount_MB:N',
            legend=None,
        )
    )

    text = bars.mark_text(
        align="left",
        baseline="middle",
        xOffset=5,
    ).encode(
        text="count:Q",
        color=alt.value("black"),
    )

    bars = text + bars

    bars.properties(
        width=500,
        height=75,
    ).save(
        "renders/package_counts.png",
        scale_factor=2,
    )

    alt.Chart(purchases).mark_bar().encode(
        x=alt.X(
            'total_GB',
            title="Total GB Purchased",
        ),
        y=alt.Y(
            'amount_MB',
            type="ordinal",
            title="Package Type (MB)",
        ),
        color=alt.Color(
            'amount_MB:N',
            legend=None,
        )
    ).properties(
        width=500,
        height=75,
    ).save(
        "renders/package_bytes.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot()

    print("Done!")
