import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.pd_infra
import bok.platform


def create_all_flows(dask_client):
    typical_flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start")[["end", "protocol", "bytes_up", "bytes_down"]]

    p2p_flows = bok.dask_infra.read_parquet("data/clean/flows/p2p_TM_DIV_none_INDEX_start")[["end", "protocol", "bytes_a_to_b", "bytes_b_to_a"]]

    nouser_flows = bok.dask_infra.read_parquet("data/clean/flows/nouser_TM_DIV_none_INDEX_start")[["end", "protocol", "bytes_a_to_b", "bytes_b_to_a"]]

    typical_flows["bytes_total"] = typical_flows["bytes_up"] + typical_flows["bytes_down"]
    p2p_flows["bytes_total"] = p2p_flows["bytes_a_to_b"] + p2p_flows["bytes_b_to_a"]
    nouser_flows["bytes_total"] = nouser_flows["bytes_a_to_b"] + nouser_flows["bytes_b_to_a"]

    typical_flows = typical_flows.reset_index()[["start", "end", "bytes_total", "protocol"]]
    p2p_flows = p2p_flows.reset_index()[["start", "end", "bytes_total", "protocol"]]
    nouser_flows = nouser_flows.reset_index()[["start", "end", "bytes_total", "protocol"]]

    all_flows = typical_flows.append(p2p_flows).append(nouser_flows)
    all_flows = all_flows.set_index("start").repartition(partition_size="128M", force=True)
    bok.dask_infra.clean_write_parquet(all_flows, "data/clean/flows/all_TM_DIV_none_INDEX_start")


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/all_TM_DIV_none_INDEX_start")[["bytes_total"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows["day"] = flows["start"].dt.day_name()
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "day"]).sum()

    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    days = ['Monday', 'Tuesday', 'Wednesday',
            'Thursday', 'Friday', 'Saturday', 'Sunday']

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    working_times = grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31"), "outage"] = "Normal"
    print(grouped_flows)

    outages = grouped_flows.loc[grouped_flows["GB"] < 0.9]
    print(outages)

    alt.Chart(working_times).mark_boxplot().encode(
        x=alt.X('day:N',
                sort=days,
                title="Day of Week"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Day"
                ),
    ).save(
        "renders/bytes_per_day_of_week_boxplot_exclude_outage.png",
        scale_factor=2,
    )

    bars = alt.Chart(working_times).mark_bar().encode(
        x=alt.X('day:N',
                sort=days,
                title="Day of Week"
                ),
        y=alt.Y('GB:Q',
                title="Mean GB Per Normal Day",
                aggregate="mean"
                ),
    )
    ci = alt.Chart(working_times).mark_errorbar(extent="ci").encode(
        x=alt.X('day:N',
                sort=days,
                title="Day of Week",
                ),
        y=alt.Y('GB:Q',
                title="(95% Bootstrap CI)",
                ),
    )
    mean_and_ci = bars + ci

    mean_and_ci.save(
        "renders/bytes_per_day_of_week_exclude_outage_mean_ci.png",
        scale_factor=2,
    )

    overplot = alt.Chart(grouped_flows).mark_point(opacity=0.3).encode(
        x=alt.X('day:N',
                sort=days,
                title="Day of Week"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Day"
                ),
        color=alt.Color(
            "outage",
            title="Condition",
        )
    )

    overplot.save(
        "renders/bytes_per_day_of_week_overplot.png",
        scale_factor=2,
    )

    (mean_and_ci | overplot).resolve_scale(y="shared").save(
        "renders/bytes_per_day_of_week_compound.png",
        scale_factor=2,
    )

    # plot = alt.Chart(grouped_flows).mark_area().encode(
    #     x=alt.X("start_bin:T",
    #             title="Time",
    #             axis=alt.Axis(labels=True),
    #             ),
    #     y=alt.Y("sum(GB):Q",
    #             title="Fraction of Traffic Per Week(GB)",
    #             stack="normalize",
    #             ),
    #     # shape="direction",
    #     color="name",
    #     detail="name",
    # ).properties(
    #     # title="Local Service Use",
    #     width=500,
    # ).save("renders/throughput_per_protocol_trends_normalized.png",
    #        scale_factor=2
    #        )


if __name__ == "__main__":
    platform = bok.platform.read_config()
    graph_temporary_file = "scratch/graphs/bytes_per_day_of_week"

    if platform.large_compute_support:
        print("Performing compute operations")
        client = bok.dask_infra.setup_dask_client()
        # create_all_flows(client)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Performing vis operations")
        make_plot(graph_temporary_file)

    print("Done!")
