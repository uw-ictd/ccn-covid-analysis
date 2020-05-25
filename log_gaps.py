""" Compute gaps where there are no flow logs in progress

Take advantage of the fact that flowlogs exist in memory before being saved.
Times where there are no logs in progress at all, across any user,
could indicate power or enodeb failures.
"""

import altair as alt
import numpy as np
import pandas as pd

import bok.constants
import bok.dask_infra
import bok.pd_infra


def reduce_flow_gaps_to_pandas(outfile, dask_client):
    typical_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "end"]]

    peer_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/p2p_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a", "end"]]

    nouser_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/nouser_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a", "end"]]

    # Compute total bytes and drop intermediates
    typical_flows["total_bytes"] = typical_flows["bytes_up"] + typical_flows["bytes_down"]
    peer_flows["total_bytes"] = peer_flows["bytes_a_to_b"] + peer_flows["bytes_b_to_a"]
    nouser_flows["total_bytes"] = nouser_flows["bytes_a_to_b"] + nouser_flows["bytes_b_to_a"]
    typical_flows = typical_flows.drop(["bytes_up", "bytes_down"], axis="columns")
    peer_flows = peer_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")
    nouser_flows = nouser_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")

    # Combine into one master flow frame
    all_flows = typical_flows.append(
        peer_flows, interleave_partitions=True
    ).append(
        nouser_flows, interleave_partitions=True
    )

    # Sort all flows by time.
    all_flows = all_flows.reset_index().set_index("start")

    # Recover the start time
    all_flows = all_flows.reset_index()

    # Manually iterate since rolling for datetimes is not implemented.
    last_end = None
    sanity_start = None
    gaps = []
    for i, row in enumerate(all_flows.itertuples()):
        if (i % 10000 == 0):
            print("Processing row {}".format(i))

        # Ensure sorted
        if (sanity_start is not None) and (row.start < sanity_start):
            print("INSANE")
            print(i)
            print(row.start)
            print(sanity_start)
            raise RuntimeError("Failed")

        if last_end is not None and row.start > last_end:
            gaps.append((last_end, row.start))

        last_end = row.end
        sanity_start = row.start

    print("Gaps length", len(gaps))
    # Gaps is small so just use pandas
    gaps_frame = pd.DataFrame(gaps, columns=["start", "end"])
    print(gaps_frame.head())

    bok.pd_infra.clean_write_parquet(gaps_frame, outfile)


def make_timeline_plot(infile):
    gaps = bok.pd_infra.read_parquet(infile)
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
    gaps = bok.pd_infra.read_parquet(infile)

    gaps["duration"] = gaps["end"] - gaps["start"]
    downtime = gaps["duration"].sum()
    total_time = bok.constants.MAX_DATE - bok.constants.MIN_DATE

    print("Downtime", downtime)
    print("Total time", total_time)
    print("Percentage", downtime/total_time * 100)


def make_outage_duration_cdf_plot(infile):
    gaps = bok.pd_infra.read_parquet(infile)

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
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/flow_gaps"
    # reduce_flow_gaps_to_pandas(outfile=graph_temporary_file, dask_client=client)
    make_timeline_plot(graph_temporary_file)
    make_outage_duration_cdf_plot(graph_temporary_file)
    compute_uptime_stats(graph_temporary_file)
