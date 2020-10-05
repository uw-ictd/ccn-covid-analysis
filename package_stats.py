"""Compute graphs and statistics of packages purchased and used
"""

import altair as alt

import bok.dask_infra
import bok.pd_infra
import bok.platform


def reduce_to_pandas(outfile, dask_client):
    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM").compute()
    bok.pd_infra.clean_write_parquet(transactions, outfile)


def make_plot(infile):
    transactions = bok.pd_infra.read_parquet(infile)

    purchases = transactions.loc[transactions["kind"] == "purchase"]
    purchases = purchases.groupby("amount_bytes")["timestamp"].count()
    purchases = purchases.reset_index().rename({"timestamp": "count"}, axis="columns")
    purchases["amount_MB"] = purchases["amount_bytes"] * 1.0/1000**2
    purchases["total_GB"] = purchases["amount_MB"] * purchases["count"] * 1.0/1000

    print(purchases)
    bars = alt.Chart(purchases).mark_bar().encode(
        x=alt.X('amount_MB',
                type="ordinal",
                title="Package Type (MB)",
                ),
        y=alt.Y(
            'count',
            title="Count",
        ),
        color=alt.Color(
            'amount_MB:N',
            legend=None,
        )
    )

    text = bars.mark_text(
        align="left",
        baseline="bottom",
    ).encode(
        text="count:Q"
    )

    bars = text + bars

    bars.save(
        "renders/package_counts.png",
        scale_factor=2,
    )

    alt.Chart(purchases).mark_bar().encode(
        x=alt.X('amount_MB',
                type="ordinal",
                title="Package Type (MB)",
                ),
        y=alt.Y(
            'total_GB',
            title="Total GB Purchased",
        ),
        color=alt.Color(
            'amount_MB:N',
            legend=None,
        )
    ).save(
        "renders/package_bytes.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()

    graph_temporary_file = "scratch/graphs/package_stats"
    if platform.large_compute_support:
        # Compute is small enough to not require a filter
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(8, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
