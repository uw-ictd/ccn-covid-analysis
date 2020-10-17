""" Computes revenue earned by the network by month
"""

import altair
import datetime
import numpy as np
import pandas as pd

import bok.constants
import bok.dask_infra
import bok.platform
import bok.pd_infra


def make_expenses():
    maintenance = pd.DataFrame({"timestamp": pd.date_range(bok.constants.MIN_DATE, bok.constants.MAX_DATE, freq="1M")})
    # Monthly power system maintenance
    maintenance = maintenance.assign(amount_idr=1300000)
    maintenance["amount_usd"] = maintenance["amount_idr"] * bok.constants.IDR_TO_USD

    vsat = pd.DataFrame({"timestamp": pd.date_range(bok.constants.MIN_DATE, bok.constants.MAX_DATE, freq="1M")})
    vsat = vsat.assign(amount_usd=300)

    initial = pd.DataFrame({
        "timestamp": [bok.constants.MIN_DATE, bok.constants.OUTAGE_END],
        "amount_usd": [9334, 1000],
    })

    expenses = initial.append(vsat).append(maintenance[["timestamp", "amount_usd"]])
    expenses = expenses.assign(kind="Cumulative Costs")

    return expenses


def reduce_to_pandas(outfile, dask_client):
    transactions = bok.dask_infra.read_parquet("data/clean/transactions_TM").compute()
    purchases = transactions.loc[(transactions["kind"] == "purchase") | (transactions["kind"] == "admin_topup")]
    purchases = purchases[["timestamp", "amount_idr", "kind", "user"]]
    bok.pd_infra.clean_write_parquet(purchases, outfile)


def make_plot(infile):
    purchases = bok.pd_infra.read_parquet(infile)
    purchases["amount_usd"] = purchases["amount_idr"] * bok.constants.IDR_TO_USD
    purchases = purchases.loc[purchases["kind"] == "admin_topup"]
    purchases = purchases.replace({"admin_topup": "Cumulative Revenue"})

    finances = purchases.append(make_expenses())

    finances = finances.set_index("timestamp").sort_index().reset_index()
    finances["amount_cum"] = finances.groupby("kind").cumsum()["amount_usd"]

    altair.Chart(finances).mark_line(interpolate='step-after').encode(
        x=altair.X("timestamp:T",
                   title="Time",
                   ),
        y=altair.Y("amount_cum",
                   title="Amount (USD)",
                   ),
        color=altair.Color("kind",
                           title="",
                           ),
    ).properties(
        width=500
    ).save(
        "renders/revenue_over_time.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()
    # pd.set_option('display.max_columns', None)

    graph_temporary_file = "scratch/graphs/revenue_vs_time"
    if platform.large_compute_support:
        print("Running compute tasks")
        print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
        client = bok.dask_infra.setup_platform_tuned_dask_client(8, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
