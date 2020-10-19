import altair as alt
import numpy as np
import pandas as pd

import bok.dask_infra
import bok.pd_infra
import bok.platform


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def make_plot(inpath):
    activity = bok.pd_infra.read_parquet(inpath)

    # Drop users that have been active less than a week.
    activity = activity.loc[
        activity["days_since_first_active"] >= 7,
    ]

    # Drop users active for less than one week
    activity = activity.loc[
        activity["days_active"] >= 1,
    ]

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    activity["optimistic_online_ratio"] = (
        np.minimum(activity["days_online"], activity["days_active"]) /
        (activity["days_active"] - activity["outage_impact_days"])
    )
    print(activity)

    # Compute a CDF since the specific user does not matter
    optimistic_cdf_frame = compute_cdf(activity, value_column="optimistic_online_ratio", base_column="user")
    optimistic_cdf_frame = optimistic_cdf_frame.rename(columns={"optimistic_online_ratio": "value"})
    optimistic_cdf_frame["type"] = "Optimistic (ignore outages)"

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    activity["observed_online_ratio"] = np.minimum(
        activity["days_online"], activity["days_active"]) / activity["days_active"]
    print(activity)

    # Compute a CDF since the specific user does not matter
    observed_cdf_frame = compute_cdf(activity, value_column="observed_online_ratio", base_column="user")
    observed_cdf_frame = observed_cdf_frame.rename(columns={"observed_online_ratio": "value"})
    observed_cdf_frame["type"] = "Observed"

    df = optimistic_cdf_frame.append(observed_cdf_frame)

    alt.Chart(df).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X('value:Q',
                scale=alt.Scale(type="linear", domain=(0, 1.00)),
                title="Online Days / Active Days"
                ),
        y=alt.Y('cdf',
                title="CDF of Users N={}".format(len(activity)),
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
        color=alt.Color(
            "type",
            legend=alt.Legend(
                title=None
            ),
        ),
        strokeDash=alt.StrokeDash(
            "type",
            legend=alt.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
            ),
        ),
    ).properties(
        width=500,
        height=200,
    ).save("renders/rate_online_when_active_cdf.png", scale_factor=2.0)


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    if platform.large_compute_support:
        print("Running compute subcommands")
        pass
        # client = bok.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        # client.close()

    if platform.altair_support:
        make_plot(inpath="data/clean/user_active_deltas.parquet")

    print("Done!")
