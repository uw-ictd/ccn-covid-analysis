import altair as alt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm

import bok.dask_infra
import bok.pd_infra
import bok.platform

def reduce_to_pandas(outpath, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "bytes_up", "bytes_down"]]

    flows["bytes_total"] = flows["bytes_up"] + flows["bytes_down"]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Do the grouping
    flows = flows.groupby(["start_bin", "user"]).sum()

    flows = flows.reset_index()[["start_bin", "user", "bytes_total"]]
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outpath)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def make_plot(inpath):
    flows = bok.pd_infra.read_parquet(inpath)
    flows = flows.reset_index()

    activity = bok.pd_infra.read_parquet("data/clean/user_active_deltas.parquet")

    # Drop users new to the network first active less than a week ago.
    activity = activity.loc[
        activity["days_since_first_active"] >= 7,
    ]
    # Drop users active for less than 1 day
    activity = activity.loc[
        activity["days_active"] >= 1,
    ]

    # take the minimum of days online and days active, since active is
    # partial-day aware, but online rounds up to whole days. Can be up to 2-e
    # days off if the user joined late in the day and was last active early.
    activity["online_ratio"] = np.minimum(
        activity["days_online"],
        (activity["days_active"] - activity["outage_impact_days"])
    ) / (activity["days_active"] - activity["outage_impact_days"])

    flows["MB"] = flows["bytes_total"] / (1000**2)
    user_total = flows[["user", "MB"]]
    user_total = user_total.groupby(["user"]).sum().reset_index()
    df = user_total.merge(
        activity[["user", "online_ratio", "days_online"]],
        on="user",
    )
    df["MB_per_online_day"] = df["MB"] / df["days_online"]

    # Log transform for analysis
    df["log_MB_per_online_day"] = df["MB_per_online_day"].map(np.log)
    df["log_online_ratio"] = df["online_ratio"].map(np.log)

    # Print log stats info
    x_log = df["log_MB_per_online_day"]
    y_log = df["log_online_ratio"]
    x_log_with_const = sm.add_constant(x_log)
    estimate = sm.OLS(y_log, x_log_with_const)
    estimate_fit = estimate.fit()
    print("Stats info for log-transformded OLS linear fit")
    print("P value", estimate_fit.pvalues[1])
    print("R squared", estimate_fit.rsquared)
    print(estimate_fit.summary())

    # Print direct linear regression stats info
    x = df["MB_per_online_day"]
    y = df["online_ratio"]
    x_with_const = sm.add_constant(x)
    estimate = sm.OLS(y, x_with_const)
    estimate_fit = estimate.fit()
    print("Stats info for direct OLS linear fit")
    print("P value", estimate_fit.pvalues[1])
    print("R squared", estimate_fit.rsquared)
    print(estimate_fit.summary())

    # Reshape to generate column matrixes expected by sklearn
    mb_array = df["MB_per_online_day"].values.reshape((-1, 1))
    online_ratio_array = df["online_ratio"].values.reshape((-1, 1))
    log_mb_array = df["log_MB_per_online_day"].values.reshape((-1, 1))
    log_online_array = df["log_online_ratio"].values.reshape((-1, 1))
    lin_regressor = LinearRegression()
    lin_regressor.fit(mb_array, online_ratio_array)
    logt_regressor = LinearRegression()
    logt_regressor.fit(log_mb_array, log_online_array)

    # Generate a regression plot
    uniform_x = np.linspace(start=mb_array.min(), stop=mb_array.max(), num=1000, endpoint=True).reshape((-1, 1))
    predictions = lin_regressor.predict(uniform_x)
    log_x = np.log(uniform_x)
    logt_predictions = logt_regressor.predict(log_x)
    logt_predictions = np.exp(logt_predictions)

    regression_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": predictions.flatten()})
    regression_frame = regression_frame.assign(type="Linear(P<0.01, R²=0.05)")

    logt_frame = pd.DataFrame({"regressionX": uniform_x.flatten(), "predictions": logt_predictions.flatten()})
    logt_frame = logt_frame.assign(type="Log Transformed Linear(P<0.005, R²=0.05)")
    regression_frame = regression_frame.append(logt_frame)

    scatter = alt.Chart(df).mark_point(opacity=0.9, strokeWidth=1.5).encode(
        x=alt.X(
            "MB_per_online_day",
            title="Mean MB per Day Online",
        ),
        y=alt.Y(
            "online_ratio",
            title="Online Days / Active Days",
            # scale=alt.Scale(type="linear", domain=(0, 1.0)),
        ),
    )

    regression = alt.Chart(logt_frame).mark_line(color="black", opacity=1).encode(
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
                orient='none',
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
                legendX=300,
                legendY=255,
            )
        ),
    )

    (scatter + regression).properties(
        width=500,
        height=250,
    ).save(
        "renders/rate_active_per_user.png", scale_factor=2.0
    )


if __name__ == "__main__":
    platform = bok.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temporary_file = "scratch/graphs/rate_active_per_user"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = bok.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(outpath=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        make_plot(inpath=graph_temporary_file)

    print("Done!")
