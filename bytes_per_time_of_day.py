import altair as alt
import bok.dask_infra
import bok.pd_infra
import bok.platform


def reduce_to_pandas(outfile, dask_client):
    typical = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "category"]]
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]
    typical = typical.reset_index()
    typical = typical[["start", "category", "bytes_total"]]

    p2p = bok.dask_infra.read_parquet(
        "data/clean/flows/p2p_TM_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a"]]
    p2p["bytes_total"] = p2p["bytes_a_to_b"] + p2p["bytes_b_to_a"]
    p2p.assign(category="Peer to Peer")
    p2p = p2p[["start", "category", "bytes_total"]]

    flows = typical.append(p2p)

    # Compress to days
    flows["day_bin"] = flows["start"].dt.floor("d")
    flows["hour"] = flows["start"].dt.hour
    flows = flows.set_index("day_bin")

    # Do the grouping
    flows = flows.groupby(["day_bin", "hour", "category"]).sum()

    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_totals_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    working_times = grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31"), "outage"] = "Normal"

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

    aggregate = working_times.groupby(["hour"]).agg({"GB": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99)]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"GB mean": "Mean",
                 "GB <lambda_0>": "90th Percentile",
                 "GB <lambda_1>": "99th Percentile",
                 })

    aggregate = aggregate.melt(
        id_vars=["hour"],
        value_vars=["Mean", "90th Percentile", "99th Percentile"],
        var_name="type",
        value_name="GB"
    )

    print(aggregate)
    # Create a hybrid chart to fix legend issue with line chart and shape
    lines = alt.Chart(aggregate).mark_line().encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Hour"
                ),
        color=alt.Color(
            "type",
            legend=None,
        ),
    )

    points = alt.Chart(aggregate).mark_point(size=100).encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Hour"
                ),
        color=alt.Color(
            "type",
        ),
        shape=alt.Shape(
            "type",
            title=""
        ),
    )

    alt.layer(
        points, lines
    ).resolve_scale(
        color='independent',
        shape='independent'
    ).save(
        "renders/bytes_per_time_of_day_lines.png",
        scale_factor=2,
    )


def make_category_plot(inpath):
    grouped_flows = bok.pd_infra.read_parquet(inpath)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["MB"] = grouped_flows["bytes_total"] / (1000**2)
    working_times = grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31"), "outage"] = "Normal"

    aggregate = working_times.groupby(["hour", "category"]).agg({"MB": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99)]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"MB mean": "Mean",
                 "MB <lambda_0>": "90th Percentile",
                 "MB <lambda_1>": "99th Percentile",
                 })

    aggregate = aggregate.melt(
        id_vars=["hour", "category"],
        value_vars=["Mean", "90th Percentile", "99th Percentile"],
        var_name="type",
        value_name="MB"
    )

    print(aggregate)
    # Create a hybrid chart to fix legend issue with line chart and shape
    lines = alt.Chart(aggregate).mark_line().encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('MB:Q',
                title="MB Per Hour Per Category"
                ),
        color=alt.Color(
            "Category:N",
            legend=None,
        ),
        stroke=alt.Stroke(
            "type",
        )
    )

    points = alt.Chart(aggregate).mark_point(size=100).encode(
        x=alt.X('hour:O',
                title="Hour of the Day"
                ),
        y=alt.Y('GB:Q',
                title="GB Per Hour"
                ),
        color=alt.Color(
            "Category:N",
        ),
        shape=alt.Shape(
            "type",
            title=""
        ),
    )

    alt.layer(
        points, lines
    ).resolve_scale(
        color='independent',
        shape='independent'
    ).save(
        "renders/bytes_per_time_of_day_category_lines.png",
        scale_factor=2,
    )



if __name__ == "__main__":
    platform = bok.platform.read_config()
    graph_temporary_file = "scratch/graphs/bytes_per_time_of_day"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_totals_plot(infile=graph_temporary_file)
        make_category_plot(inpath=graph_temporary_file)

    print("Done!")
