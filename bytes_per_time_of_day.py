import altair as alt
import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    typical = infra.dask.read_parquet(
        "data/clean/flows_typical_DIV_none_INDEX_start"
    )[["bytes_up", "bytes_down", "category"]]
    typical["bytes_total"] = typical["bytes_up"] + typical["bytes_down"]
    typical = typical.reset_index()
    typical = typical[["start", "category", "bytes_total"]]

    p2p = infra.dask.read_parquet(
        "data/clean/flows_p2p_DIV_none_INDEX_start"
    )[["bytes_a_to_b", "bytes_b_to_a"]]
    p2p["bytes_total"] = p2p["bytes_a_to_b"] + p2p["bytes_b_to_a"]
    p2p = p2p.assign(category="Peer to Peer")
    p2p = p2p.categorize(columns=["category"])
    p2p = p2p.reset_index()
    p2p = p2p[["start", "category", "bytes_total"]]

    flows = typical.append(p2p)

    # Compress to days
    flows["day_bin"] = flows["start"].dt.floor("d")
    flows["hour"] = flows["start"].dt.hour
    flows = flows.set_index("day_bin")

    # Do the grouping
    flows = flows.groupby(["day_bin", "hour", "category"]).sum()

    flows = flows.compute()

    infra.pd.clean_write_parquet(flows, outfile)


def make_totals_plot(infile):
    grouped_flows = infra.pd.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["MB"] = grouped_flows["bytes_total"] / (1000**2)
    working_times = grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31")]
    grouped_flows["outage"] = "Outage"
    grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31"), "outage"] = "Normal"

    alt.Chart(working_times).mark_boxplot().encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'MB:Q',
            title="MB Per Hour",
        ),
    ).save(
        "renders/bytes_per_time_of_day_boxplot_exclude_outage.png",
        scale_factor=2,
    )

    alt.Chart(grouped_flows).mark_point(opacity=0.3).encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'MB:Q',
            title="MB Per Hour",
        ),
        color=alt.Color(
            "outage",
            title="Condition",
        )
    ).save(
        "renders/bytes_per_time_of_day_overplot.png",
        scale_factor=2,
    )

    # Sum across categories, then compute statistics across all days.
    aggregate = working_times.groupby(["hour", "day_bin"]).sum()
    aggregate = aggregate.groupby(["hour"]).agg({"MB": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99), "max"]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"MB mean": "Mean",
                 "MB <lambda_0>": "90th Percentile",
                 "MB <lambda_1>": "99th Percentile",
                 "MB max": "Max",
                 })

    aggregate = aggregate.melt(
        id_vars=["hour"],
        value_vars=["99th Percentile", "90th Percentile", "Mean"],
        # value_vars=["Mean", "90th Percentile", "99th Percentile", "Max"],
        var_name="type",
        value_name="MB"
    )

    print(aggregate)
    # Create a hybrid chart to fix legend issue with line chart and shape
    lines = alt.Chart(aggregate).mark_line().encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'MB:Q',
            title="MB Per Hour",
        ),
        color=alt.Color(
            "type",
            legend=None,
            sort=None,
        ),
    )

    points = alt.Chart(aggregate).mark_point(size=100).encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'MB:Q',
            title="MB Per Hour",
        ),
        color=alt.Color(
            "type",
            legend=alt.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
            ),
            sort=None,
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
    ).properties(
        width=500,
        height=250,
    ).save(
        "renders/bytes_per_time_of_day_lines.png",
        scale_factor=2,
    )


def make_category_plot(inpath):
    grouped_flows = infra.pd.read_parquet(inpath)
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

    aggregate = aggregate.loc[aggregate["type"] == "Mean"]

    alt.Chart(aggregate).mark_line().encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'MB:Q',
            title="Avg MB Per Hour Per Category",
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
            sort=['Video', 'Social Media', 'Messaging', 'Mixed CDN', 'Unknown (No DNS)', 'Software or Updates', 'API', 'Unknown (Not Mapped)', 'Adult Video', 'ICE (STUN/TURN)', 'Peer to Peer', 'Ad Network', 'Non-video Content', 'Compressed Web', 'Games', 'Local Services', 'Content Upload', 'IAAS', 'Files', 'Antivirus']
        ),
        # strokeDash=alt.StrokeDash(
        #     "category:N",
        # ),
    ).save(
        "renders/bytes_per_time_of_day_category_lines.png",
        scale_factor=2,
    )


def make_change_vs_average_plot(inpath):
    grouped_flows = infra.pd.read_parquet(inpath)
    grouped_flows = grouped_flows.reset_index()

    grouped_flows["MB"] = grouped_flows["bytes_total"] / (1000**2)
    working_times = grouped_flows.loc[(grouped_flows["day_bin"] < "2019-07-30") | (grouped_flows["day_bin"] > "2019-08-31")]

    aggregate = working_times.groupby(["hour", "category"]).agg({"MB": "sum"})
    aggregate = aggregate.reset_index()
    category_total = working_times.groupby(["category"]).sum()
    category_total = category_total.reset_index()[["category", "MB"]]
    category_total = category_total.rename(columns={"MB": "category_total_MB"})

    aggregate = aggregate.merge(category_total, on="category")
    aggregate["byte_density"] = aggregate["MB"] / aggregate["category_total_MB"]

    print(aggregate)
    print(category_total)

    alt.Chart(aggregate).mark_line().encode(
        x=alt.X(
            'hour:O',
            title="Hour of the Day",
        ),
        y=alt.Y(
            'byte_density:Q',
            title="Fraction of Category Bytes Per Hour",
        ),
        color=alt.Color(
            "category:N",
            scale=alt.Scale(scheme="tableau20"),
        ),
        strokeDash=alt.StrokeDash(
            "category:N",
        ),
    ).save(
        "renders/bytes_per_time_of_day_category_relative_shift.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = infra.platform.read_config()
    graph_temporary_file = "scratch/graphs/bytes_per_time_of_day"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_change_vs_average_plot(inpath=graph_temporary_file)
        make_totals_plot(infile=graph_temporary_file)
        make_category_plot(inpath=graph_temporary_file)

    print("Done!")
