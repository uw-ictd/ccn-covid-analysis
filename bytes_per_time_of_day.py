import altair as alt
import bok.dask_infra
import bok.pd_infra


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/all_TM_DIV_none_INDEX_start")[["bytes_total"]]

    # Compress to days
    flows = flows.reset_index()
    flows["day_bin"] = flows["start"].dt.floor("d")
    flows["hour"] = flows["start"].dt.hour
    flows = flows.set_index("day_bin")

    # Do the grouping
    flows = flows.groupby(["day_bin", "hour"]).sum()

    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    working_times = grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["start_bin"] < "2019-07-30") | (grouped_flows["start_bin"] > "2019-08-31"), "outage"] = "Normal"
    print(grouped_flows)

    outages = grouped_flows.loc[grouped_flows["GB"] < 0.9]
    print(outages)

    alt.Chart(working_times).mark_boxplot().encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Hour"
                ),
    ).save(
        "renders/bytes_per_time_of_day_boxplot_exclude_outage.png",
        scale_factor=2,
    )

    alt.Chart(grouped_flows).mark_point(opacity=0.3).encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Hour"
                ),
        color=alt.Color(
            "outage",
            title="Condition",
        )
    ).save(
        "renders/bytes_per_time_of_day_overplot.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/bytes_per_time_of_day"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    make_plot(infile=graph_temporary_file)
