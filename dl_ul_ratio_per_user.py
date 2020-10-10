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


def _assign_user_top_category(frame):
    print(frame)


def make_ul_dl_scatter_plot(infile):
    user_totals = bok.pd_infra.read_parquet(infile)
    user_totals = user_totals.reset_index()

    user_totals = user_totals.groupby(["user"]).sum().reset_index()
    user_totals["bytes_total"] = user_totals["bytes_up"] + user_totals["bytes_down"]

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

    user_totals["log_ul_ratio"] = user_totals["ul ratio"].map(np.log)
    user_totals["log_mb_per_day"] = user_totals["MB_avg_per_online_day"].map(np.log)

    # Perform Statistical Regression
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

    graph_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": predictions.flatten()})
    graph_frame = graph_frame.assign(type="Linear")
    graph_frame = graph_frame.append(user_totals)

    logt_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": logt_predictions.flatten()})
    logt_frame = logt_frame.assign(type="Semi-Log Transformed")
    graph_frame = graph_frame.append(logt_frame)

    scatter = alt.Chart(graph_frame).mark_point().encode(
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
    )

    regression = scatter.mark_line(color="orange").encode(
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
        )
    )

    (scatter + regression).properties(
        width=500,
    ).save(
        "renders/dl_ul_ratio_per_user_scatter.png",
        scale_factor=2,
    )

    X = user_totals["log_mb_per_day"]
    Y = user_totals["log_ul_ratio"]
    X2 = sm.add_constant(X)
    est = sm.OLS(Y, X2)
    est2 = est.fit()
    print(est2.pvalues)
    print(est2.summary())


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
        pd.set_option('display.max_rows', None)
        make_ul_dl_scatter_plot(graph_temporary_file)
        # ToDo Need to add statistical analysis of the trend.

    print("Done!")
