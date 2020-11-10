import altair as alt
import pandas as pd

import infra.constants
import infra.dask
import infra.pd


def make_timeline_plot(infile):
    gaps = infra.pd.read_parquet(infile)
    print(gaps.head())
    alt.Chart(gaps).mark_rect().encode(
        x=alt.X("start:T"),
        x2=alt.X2("end:T"),
    ).properties(
        width=1000,
    ).save(
        "renders/log_gaps_downtime.png",
        scale_factor=2,
    )


def compute_uptime_stats(infile):
    gaps = infra.pd.read_parquet(infile)

    gaps["duration"] = gaps["end"] - gaps["start"]
    downtime = gaps["duration"].sum()
    total_time = infra.constants.MAX_DATE - infra.constants.MIN_DATE

    print("Downtime", downtime)
    print("Total time", total_time)
    print("Percentage", downtime/total_time * 100)


def make_outage_duration_cdf_plot(infile):
    gaps = infra.pd.read_parquet(infile)

    gaps["duration"] = gaps["end"] - gaps["start"]

    # Only record as an outage if the duration is longer than 10s. This is
    # shorter than the reasonable recovery time of the epc. Shorter gaps are
    # possible in the normal flow of traffic, especially at lightly loaded
    # hours when there may only be a few flows and low overlap.
    gaps = gaps.loc[gaps["duration"] >= pd.Timedelta(seconds=10)]

    # Convert to Hours
    gaps["duration_hours"] = gaps["duration"].dt.total_seconds() / 3600

    stats_frame = compute_cdf(gaps, "duration_hours", "start")

    alt.Chart(stats_frame).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X('duration_hours:Q',
                scale=alt.Scale(type="log"),
                title="Duration of Outage (Hours)"
                ),
        y=alt.Y('cdf',
                title="Fraction of Recorded Outages (CDF)",
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
    ).properties(
        width=500,
    ).save("renders/log_gaps_outage_duration_cdf.png", scale_factor=2.0)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


if __name__ == "__main__":
    client = infra.dask.setup_dask_client()
    log_gaps_file = "data/derived/log_gaps_TM.parquet"
    make_timeline_plot(log_gaps_file)
    make_outage_duration_cdf_plot(log_gaps_file)
    compute_uptime_stats(log_gaps_file)
