""" Computing active and registered users on the network over time
"""

import altair as alt
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


HEALTH_FQDN_TOKENS = {
    "infeksi",
    "kemkes",
    "covid",
    "corona",
    "korona",
    "kesehatan",
    "kesegaran",
    "kewarasan",
    "dokter",
    "doktor",
    "pengobat",
    "mengobati",
    "merawat",
    "penyakit",
    "who",
    "health",
    "virus",
    "vaccine",
    "vaksin"
}


def categorize_all_fqdns(df):
    """Categorizes an fqdn, currently as health or not health
    """

    def _categorize_health(fqdn):
        test_string = fqdn.lower()
        matches = filter(lambda x: x in test_string, HEALTH_FQDN_TOKENS)

        matched = False
        for match in matches:
            matched = True
            break

        return matched

    df["is_health"] = df["fqdn"].apply(_categorize_health)

    return df


def make_health_domain_plots(infile):
    """ Top-level plotting function to generate multiple related plots from the same annotated dataset.
    """
    df = infra.pd.read_parquet(infile)
    df = categorize_all_fqdns(df)
    temp = df.loc[df["is_health"]]
    print(temp["fqdn"].unique())
    _make_volume_comparison_plot(df)
    _make_relative_volume_plot(df)
    _make_unique_users_plot(df)
    _make_individual_domain_plot(df)


def _make_individual_domain_plot(df):
    df["GB_total"] = (df["bytes_up"] + df["bytes_down"])/(1000**3)
    df = df.loc[df["is_health"] == True]

    # Consolidate by week instead of by day
    df = df[
        ["start_bin", "fqdn", "GB_total"]
    ].groupby(
        [pd.Grouper(key="start_bin", freq="W-MON"), "fqdn"]
    ).sum().reset_index()

    alt.Chart(df).mark_line(opacity=1.0).encode(
        x=alt.X(
            "start_bin:T"
        ),
        y=alt.Y(
            "GB_total:Q",
            title="Weekly Total Traffic (GB)",
            axis=alt.Axis(labels=True),
            # scale=alt.Scale(
            #     type="symlog"
            # ),
        ),
        color=alt.Color(
            "fqdn:N"
        ),
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_individual_domain_weekly.png",
        scale_factor=2,
    )


def _make_unique_users_plot(df):
    df = df.loc[df["is_health"] == True]
    # Consolidate by week instead of by day
    print(df)
    temp_count = df.loc[df["fqdn"] == "www.alodokter.com"]
    print(temp_count)

    df = df[
        ["start_bin", "user"]
    ].groupby(
        [pd.Grouper(key="start_bin", freq="W-MON")]
    ).count().reset_index()

    alt.Chart(df).mark_line(opacity=1.0, interpolate='step-after').encode(
        x=alt.X(
            "start_bin:T"
        ),
        y=alt.Y(
            "user:Q",
            title="Weekly Unique Users",
            axis=alt.Axis(labels=True),
            # scale=alt.Scale(
            #     type="symlog"
            # ),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_unique_users.png",
        scale_factor=2,
    )


def _make_relative_volume_plot(df):
    df = df.loc[df["is_health"] == True]
    # Consolidate by week instead of by day
    df = df[
        ["start_bin", "bytes_up", "bytes_down"]
    ].groupby(
        [pd.Grouper(key="start_bin", freq="W-MON")]
    ).sum().reset_index()

    df["bytes"] = df["bytes_up"] + df["bytes_down"]

    alt.Chart(df).mark_line(opacity=1.0, interpolate='step-after').encode(
        x=alt.X(
            "start_bin:T"
        ),
        y=alt.Y(
            "bytes:Q",
            title="Weekly Traffic(B)",
            axis=alt.Axis(labels=True),
            # scale=alt.Scale(
            #     type="symlog"
            # ),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_isolated_magnitude.png",
        scale_factor=2,
    )

def _make_volume_comparison_plot(df):
    # Consolidate by week instead of by day
    df = df[
        ["start_bin", "is_health", "bytes_up", "bytes_down"]
    ].groupby(
        [pd.Grouper(key="start_bin", freq="W-MON"), "is_health"]
    ).sum().reset_index()

    df = df.melt(
        id_vars=["is_health", "start_bin"],
        value_vars=["bytes_up", "bytes_down"],
        var_name="direction",
        value_name="bytes"
        )

    df["bytes"] = df["bytes"] / 1000**3

    alt.Chart(df).mark_point(opacity=1.0).encode(
        x=alt.X(
            "start_bin:T"
        ),
        y=alt.Y(
            "bytes:Q",
            title="Weekly Traffic(GB)",
            axis=alt.Axis(labels=True),
            scale=alt.Scale(
                type="symlog"
            ),
        ),
        # shape="direction",
        color=alt.Color(
            "direction",
            title="Type",
        ),
        shape=alt.Shape(
            "is_health:N"
        )
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_overall_magnitude.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    platform = infra.platform.read_config()

    graph_temporary_file = "data/aggregates/bytes_per_user_per_domain_per_day.parquet"
    if platform.large_compute_support:
        pass

    if platform.altair_support:
        make_health_domain_plots(graph_temporary_file)
        # make_org_plot(graph_temporary_file)
        # make_category_aggregate_bar_chart(graph_temporary_file)
