import altair as alt
import bok.dask_infra
import bok.pd_infra
import numpy as np
import pandas as pd


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_org_local_TM_DIV_none_INDEX_start")[["user", "bytes_up", "bytes_down"]]

    flows["bytes_total"] = flows["bytes_up"] + flows["bytes_down"]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "user"]).sum()

    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def make_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    working_times = grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31"), "outage"] = "Normal"


    aggregate = clean_purchases.groupby(["user"]).agg({"time_since_last_purchase": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99)]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"time_since_last_purchase mean": "mean",
                 "time_since_last_purchase <lambda_0>": "q90",
                 "time_since_last_purchase <lambda_1>": "q99",
                 })

    # Compute a CDF since the specific user does not matter
    stats_mean = compute_cdf(aggregate, "mean", "user")
    stats_mean = stats_mean.rename(columns={"mean": "value"})
    stats_mean["type"] = "User's Mean"

    stats_q90 = compute_cdf(aggregate, "q90", "user")
    stats_q90 = stats_q90.rename(columns={"q90": "value"})
    stats_q90["type"] = "User's 90% Quantile"

    stats_q99 = compute_cdf(aggregate, "q99", "user")
    stats_q99 = stats_q99.rename(columns={"q99": "value"})
    stats_q99["type"] = "User's 99% Quantile"

    stats_frame = stats_mean.append(stats_q90).append(stats_q99)

    alt.Chart(stats_frame).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X('value:Q',
                scale=alt.Scale(type="linear", domain=(0.01, 80)),
                title="GB per Active Day (Hours)"
                ),
        y=alt.Y('cdf',
                title="Fraction of Users (CDF)",
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
        color="type",
        strokeDash="type",
    ).save("renders/bytes_per_active_day_per_user_cdf.png", scale_factor=2.0)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/bytes_per_active_day_per_user"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    make_plot(graph_temporary_file)
