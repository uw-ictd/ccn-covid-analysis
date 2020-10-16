""" Analyze the ratio of uplink and downlink per user
"""

import altair as alt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from sklearn.metrics import r2_score

import bok.dask_infra
import bok.domains
import bok.pd_infra
import bok.platform


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start"
    )[["user", "bytes_up", "bytes_down", "category"]]

    # Do the grouping
    flows = flows.groupby(["user", "category"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def _find_user_top_category(df):
    """ Assigns each user a column with the label of their top category"""
    df["total_bytes"] = df["bytes_up"] + df["bytes_down"]
    df["category_and_amount"] = df.apply(lambda row: (row["category"], row["total_bytes"]), axis=1)

    def _find_label_with_max_value(series):
        # Some messy accessors because the series iterator returns (index, (category, amount))
        return max(series.iteritems(), key=lambda x: x[1][1])[1][0]

    user_top = df.groupby(["user"]).agg({"category_and_amount": _find_label_with_max_value})
    user_top = user_top.rename(columns={"category_and_amount": "top_category"})
    user_top = user_top.groupby(["user"]).first()
    return user_top


def make_ul_dl_scatter_plot(infile):
    user_cat = bok.pd_infra.read_parquet(infile)
    user_cat = user_cat.reset_index()

    # Filter users to only users who made purchases in the network with registered ips
    users = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")[["user"]]
    user_cat = users.merge(user_cat, on="user", how="left")

    # Compute total bytes for each user across categories
    user_totals = user_cat.groupby(["user"]).sum().reset_index()
    user_totals["bytes_total"] = user_totals["bytes_up"] + user_totals["bytes_down"]

    user_cat = _find_user_top_category(user_cat)
    print(user_cat)

    user_totals = user_totals.merge(user_cat, on="user")
    print(user_totals)

    # Filter users by time in network to eliminate early incomplete samples
    user_active_ranges = bok.pd_infra.read_parquet(
        "data/clean/user_active_deltas.parquet")[["user", "days_since_first_active", "days_active", "days_online"]]
    # Drop users that joined less than a week ago.
    users_to_analyze = user_active_ranges.loc[
        user_active_ranges["days_since_first_active"] > 7
        ]
    # Drop users active for less than one day
    users_to_analyze = users_to_analyze.loc[
        users_to_analyze["days_active"] > 1,
    ]

    user_totals = user_totals.merge(users_to_analyze, on="user", how="inner")

    # Rank users by their online daily use.
    user_totals["bytes_avg_per_online_day"] = user_totals["bytes_total"] / user_totals["days_online"]
    user_totals["rank_total"] = user_totals["bytes_total"].rank(method="min", pct=False)
    user_totals["rank_daily"] = user_totals["bytes_avg_per_online_day"].rank(method="min", pct=False)

    # Normalize ul and dl by days online
    user_totals["bytes_up_avg_per_online_day"] = user_totals["bytes_up"] / user_totals["days_online"]
    user_totals["bytes_down_avg_per_online_day"] = user_totals["bytes_down"] / user_totals["days_online"]

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    user_totals["normalized_days_online"] = np.minimum(
        user_totals["days_online"], user_totals["days_active"]) / user_totals["days_active"]

    user_totals["MB_avg_per_online_day"] = user_totals["bytes_avg_per_online_day"] / (1000**2)
    user_totals["ul ratio"] = user_totals["bytes_up"] / user_totals["bytes_total"]
    user_totals["dl ratio"] = user_totals["bytes_down"] / user_totals["bytes_total"]

    # Perform Regressions and Stats Analysis
    # Log-transform to analyze exponential relationships with linear regression
    user_totals["log_ul_ratio"] = user_totals["ul ratio"].map(np.log)
    user_totals["log_mb_per_day"] = user_totals["MB_avg_per_online_day"].map(np.log)

    # Print log stats info
    x_log = user_totals["log_mb_per_day"]
    y_log = user_totals["log_ul_ratio"]
    x_log_with_const = sm.add_constant(x_log)
    estimate = sm.OLS(y_log, x_log_with_const)
    estimate_fit = estimate.fit()
    print("Stats info for log-transformded OLS linear fit")
    print("P value", estimate_fit.pvalues[1])
    print("R squared", estimate_fit.rsquared)
    print(estimate_fit.summary())

    # Print direct linear regression stats info
    x = user_totals["MB_avg_per_online_day"]
    y = user_totals["ul ratio"]
    x_with_const = sm.add_constant(x)
    estimate = sm.OLS(y, x_with_const)
    estimate_fit = estimate.fit()
    print("Stats info for direct OLS linear fit")
    print("P value", estimate_fit.pvalues[1])
    print("R squared", estimate_fit.rsquared)
    print(estimate_fit.summary())

    # Reshape to generate column matrixes expected by sklearn
    mb_array = user_totals["MB_avg_per_online_day"].values.reshape((-1, 1))
    ul_ratio_array = user_totals["ul ratio"].values.reshape((-1, 1))
    log_mb_array = user_totals["log_mb_per_day"].values.reshape((-1, 1))
    log_ul_array = user_totals["log_ul_ratio"].values.reshape((-1, 1))
    lin_regressor = LinearRegression()
    lin_regressor.fit(mb_array, ul_ratio_array)
    logt_regressor = LinearRegression()
    logt_regressor.fit(log_mb_array, log_ul_array)

    # Generate a regression plot
    uniform_x = np.linspace(start=mb_array.min(), stop=mb_array.max(), num=1000, endpoint=True).reshape((-1, 1))
    predictions = lin_regressor.predict(uniform_x)
    log_x = np.log(uniform_x)
    logt_predictions = logt_regressor.predict(log_x)
    logt_predictions = np.exp(logt_predictions)

    regression_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": predictions.flatten()})
    regression_frame = regression_frame.assign(type="Linear(P<0.0001, R²=0.09)")

    logt_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": logt_predictions.flatten()})
    logt_frame = logt_frame.assign(type="Log Transformed Linear(P<0.0001, R²=0.19)")
    regression_frame = regression_frame.append(logt_frame)

    user_totals = user_totals.groupby(["user"]).first()

    scatter = alt.Chart(user_totals).mark_point(opacity=0.9, strokeWidth=1.5).encode(
        x=alt.X(
            "MB_avg_per_online_day:Q",
            title="User's Average MB Per Day Online",
            # scale=alt.Scale(
            #     type="log",
            # ),
        ),
        y=alt.Y(
            "ul ratio:Q",
            title="Uplink/Total Bytes Ratio",
            # scale=alt.Scale(
            #     type="log",
            # ),
        ),
        # color=alt.Color(
        #     "top_category",
        #     scale=alt.Scale(scheme="category20"),
        #     sort="descending",
        # ),
        # shape=alt.Shape(
        #     "top_category",
        #     sort="descending",
        # )
    )

    regression = alt.Chart(regression_frame).mark_line(color="black", opacity=1).encode(
        x=alt.X(
            "regressionX",
            # scale=alt.Scale(
            #     type="log",
            # ),
        ),
        y=alt.Y(
            "predictions",
            # scale=alt.Scale(
            #     type="log",
            # ),
        ),
        strokeDash=alt.StrokeDash(
            "type",
            title=None,
            legend=alt.Legend(
                orient="top-right",
                fillColor="white",
                labelLimit=500,
                padding=10,
                strokeColor="black",
            )
        )
    )

    (regression + scatter).properties(
        width=500,
    ).save(
        "renders/dl_ul_ratio_per_user_scatter.png",
        scale_factor=2,
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()
    graph_temporary_file = "scratch/graphs/dl_ul_ratio_per_user"

    if platform.large_compute_support:
        print("Running compute tasks")
        client = bok.dask_infra.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        # Module specific format options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_rows', 40)
        make_ul_dl_scatter_plot(graph_temporary_file)
        # ToDo Need to add statistical analysis of the trend.

    print("Done!")
