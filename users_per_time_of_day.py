import altair as alt
import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    flows = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start")[["user"]]

    # Compress to days
    flows = flows.reset_index()
    flows["day_bin"] = flows["start"].dt.floor("d")
    flows["hour"] = flows["start"].dt.hour
    flows = flows.set_index("day_bin")

    # Do a two level grouping to eliminate duplicate entries for each user flow.
    flows = flows.groupby(["day_bin", "hour", "user"]).sum()
    flows = flows.reset_index()
    flows = flows.groupby(["day_bin", "hour"]).count()

    flows = flows.compute()

    infra.pd.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = infra.pd.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    working_times = grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31"), "outage"] = "Normal"

    alt.Chart(working_times).mark_boxplot().encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('user:Q',
                title="Active User Count"
                ),
    ).save(
        "renders/users_per_time_of_day_boxplot_exclude_outage.png",
        scale_factor=2,
    )

    alt.Chart(grouped_flows).mark_point(opacity=0.1).encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('user:Q',
                title="Active User Count"
                ),
        color=alt.Color(
            "outage",
            title="Condition",
        )
    ).save(
        "renders/users_per_time_of_day_overplot.png",
        scale_factor=2,
    )

    aggregate = working_times.groupby(["hour"]).agg({"user": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99), "max"]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"user mean": "Mean",
                 "user <lambda_0>": "90th Percentile",
                 "user <lambda_1>": "99th Percentile",
                 "user max": "Max",
                 })

    aggregate = aggregate.melt(
        id_vars=["hour"],
        value_vars=["Max", "99th Percentile", "90th Percentile", "Mean"],
        var_name="type",
        value_name="user"
    )

    print(aggregate)
    # Create a hybrid chart to fix legend issue with line chart and shape
    lines = alt.Chart(aggregate).mark_line().encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'user:Q',
            title="Active User Count",
        ),
        color=alt.Color(
            "type",
            legend=None,
            sort=None,
        ),
    )

    points = lines.mark_point(size=100).encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'user:Q',
            title="Active User Count",
        ),
        color=alt.Color(
            "type",
            sort=None,
            legend=alt.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
            ),
        ),
        shape=alt.Shape(
            "type",
            title="",
            sort=None,
        ),
    )

    alt.layer(
        points, lines
    ).resolve_scale(
        color='independent',
        shape='independent'
    ).save(
        "renders/users_per_time_of_day_lines.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()
    graph_temporary_file = "scratch/graphs/users_per_time_of_day"

    if platform.large_compute_support:
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        make_plot(infile=graph_temporary_file)

    print("Done!")
