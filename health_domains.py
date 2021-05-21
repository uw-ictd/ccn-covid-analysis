""" Computing active and registered users on the network over time
"""
from datetime import datetime, timedelta

import altair as alt
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


# Contact tracing apps: PeduliLindungi, Bersatu Lawan COVID-19,

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
    "vaksin",
    "PeduliLindungi",
    "peduli",
    "lindungi",
    "medical",
    "telemedicine",
    "medicine",
    "obat"
}

FALSE_POSITIVE_EXLUSIONS = {
    "wholesale", # Ad network, not WHO : )
    "antivirus", # Computer antivirus
    "mobihealthplus.com.", # Antivirus
    "bras-base-hpwhon5203w-grc-03-65-94-57-54.dsl.bell.ca", # Telecom
    "the-who-8.core.ynet.pl", # Telecom
    "adminmusic-test.mobihealthplus.com",
    "whos.amung.us", # Analytics
    "samsunghealth.com", # Phone personal health tracking
    "samsungcloud.com", # Phone personal health tracking
    "coronalabs.com", # Online gaming
    "unhealthyrange.com", # Generic example code generating bad domain???
    "bpjs-kesehatan.go.id", # Public social welfare program cash transfer
    "wholeless-mileage.volia.net", # No response, seems not health related
    "whoareyou.ff.garena.com", # Game survey
    "hologfx.com", # Anime media sharing
    "acrobat.com", # Not medicine in indonesian : )
    "adobe.com", # Related to acrobat
    "tanobato.wordpress.com", # Religious blog
    "sobatdrama.net.", # Media
    "sobatkerja.zendesk.com.", # Sobat != obat
    "sobatmateri.com", # sobat != obat
    "sobatnewss.blogspot.com.", # sobat != obat
    "sobat-otomotif.blogspot.com", # sobat != obat
    "comm.miui.com.", # alternative android UI
    "jualobatpembesarpenisherbalklg.blogspot.com.", # Spam "male enhancement health"
    "dunia-kesehatan01.blogspot.com.", # Blog removed and address seized!
    "kesehatan.kontan.co.id.", # Indonesian news website health section
    "health.detik.com.", # Indonesian news website health section
    "health.kompas.com.", # News website health section
    "healthcareitnews.com.", # News website

}

def _contains_any_token(test_string, token_list):
    matches = filter(lambda x: x in test_string, token_list)

    matched = False
    for match in matches:
        matched = True
        break

    return matched


def _make_primary_domain_from_fqdn(fqdn):
    parts = fqdn.strip().strip(".").split(".")

    # Special case for amp domains
    if len(parts) == 4 and parts[1:4] == ["cdn", "ampproject", "org"]:
        source_host = parts[0].split("-")
        parts = source_host

    # Special case for alodokter firebase links
    if fqdn == "alodokter.page.link." or fqdn == "alodokterdev.page.link." or fqdn == "alodokterstaging.page.link.":
        return "alodokter.com."

    # Special case for onesignal.com
    if fqdn == "doktersehat.onesignal.com.":
        return "doktersehat.com."

    # All country code TLDs are two characters long, and will have sub-tlds (i.e. .co.uk ~ .com)
    is_cc_domain = bool(len(parts[-1]) == 2)

    if is_cc_domain:
        return ".".join(parts[max(-len(parts), -3):]) + "."
    else:
        return ".".join(parts[max(-len(parts), -2):]) + "."


def categorize_all_fqdns(df):
    """Categorizes an fqdn, currently as health or not health
    """

    def _categorize_health(fqdn):
        test_string = fqdn.lower()

        return bool(
            _contains_any_token(test_string, HEALTH_FQDN_TOKENS) and
            not _contains_any_token(test_string, FALSE_POSITIVE_EXLUSIONS)
        )

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
    _make_individual_domain_unique_users_plot(df)


def _make_individual_domain_unique_users_plot(df):
    df["GB_total"] = (df["bytes_up"] + df["bytes_down"])/(1000**3)
    df["primary_domain"] = df["fqdn"].apply(_make_primary_domain_from_fqdn)

    df = df.loc[df["is_health"] == True]

    # Consolidate by week instead of by day
    # df = df[
    #     ["day", "primary_domain", "user"]
    # ].groupby(
    #     [pd.Grouper(key="day", freq="W-MON"), "primary_domain"]
    # )["user"].nunique().reset_index()
    df = df[
        ["day", "primary_domain", "user"]
    ].groupby(
        [pd.Grouper(key="day", freq="W-MON"), "primary_domain"]
    )

    df = df["user"].nunique().reset_index()

    week_range = pd.DataFrame({"day": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE + timedelta(days=1), freq="W-MON")})
    domain_range = pd.DataFrame({"primary_domain": df["primary_domain"].unique()}, dtype=object)
    dense_index = infra.pd.cartesian_product(week_range, domain_range)

    df = df.merge(dense_index, on=["day", "primary_domain"], how="outer")
    df = df.fillna(0)

    df["timedelta"] = (df["day"] - datetime.strptime('2020-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')).dt.to_pytimedelta()
    df["week_number"] = df["timedelta"].apply(lambda x: int(x.days/7))
    df = df.drop("timedelta", axis=1)

    alt.Chart(df).mark_line(opacity=0.5, interpolate='step-after').encode(
        x=alt.X(
            "day:T"
        ),
        y=alt.Y(
            "user",
            title="Weekly Unique Users",
            axis=alt.Axis(labels=True),
            # scale=alt.Scale(
            #     type="symlog"
            # ),
        ),
        color=alt.Color(
            "primary_domain:N",
            scale=alt.Scale(scheme="tableau20"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_individual_domain_weekly.png",
        scale_factor=2,
    )

    alt.Chart(df).mark_rect(opacity=1.0).encode(
        x=alt.X(
            "week_number:O",
        ),
        y=alt.Y(
            "primary_domain:N",
            title="Weekly Unique Users",
            # axis=alt.Axis(labels=True),
            # scale=alt.Scale(
            #     type="symlog"
            # ),
        ),
        color=alt.Color(
            "user:Q",
            # scale=alt.Scale(scheme="tableau20"),
        ),
    ).properties(
        width=500,
    ).save(
        "renders/health_domain_individual_domain_weekly_heatmap.png",
        scale_factor=2,
    )


def _make_individual_domain_plot(df):
    df["GB_total"] = (df["bytes_up"] + df["bytes_down"])/(1000**3)
    df["primary_domain"] = df["fqdn"].apply(_make_primary_domain_from_fqdn)

    df = df.loc[df["is_health"] == True]

    # Consolidate by week instead of by day
    df = df[
        ["day", "primary_domain", "GB_total"]
    ].groupby(
        [pd.Grouper(key="day", freq="W-MON"), "primary_domain"]
    ).sum().reset_index()

    alt.Chart(df).mark_line(opacity=1.0).encode(
        x=alt.X(
            "day:T"
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
            "primary_domain:N",
            scale=alt.Scale(scheme="tableau20"),
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
        ["day", "user"]
    ].groupby(
        [pd.Grouper(key="day", freq="W-MON")]
    )["user"].nunique().reset_index()

    alt.Chart(df).mark_line(opacity=1.0, interpolate='step-after').encode(
        x=alt.X(
            "day:T"
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
        ["day", "bytes_up", "bytes_down"]
    ].groupby(
        [pd.Grouper(key="day", freq="W-MON")]
    ).sum().reset_index()

    df["bytes"] = df["bytes_up"] + df["bytes_down"]

    alt.Chart(df).mark_line(opacity=1.0, interpolate='step-after').encode(
        x=alt.X(
            "day:T"
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
        ["day", "is_health", "bytes_up", "bytes_down"]
    ].groupby(
        [pd.Grouper(key="day", freq="W-MON"), "is_health"]
    ).sum().reset_index()

    df = df.melt(
        id_vars=["is_health", "day"],
        value_vars=["bytes_up", "bytes_down"],
        var_name="direction",
        value_name="bytes"
        )

    df["bytes"] = df["bytes"] / 1000**3

    alt.Chart(df).mark_point(opacity=1.0).encode(
        x=alt.X(
            "day:T"
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
