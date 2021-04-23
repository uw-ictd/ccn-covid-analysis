""" Computing active and registered users on the network over time
"""

import altair
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform


def reduce_to_pandas(outfile, dask_client):
    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.
    df = infra.dask.read_parquet("data/clean/flows_typical_DIV_none_INDEX_start")

    df = df.reset_index()
    df["day"] = df["start"].dt.floor("d")
    df["user"] = df["user"].astype(object)

    # Group by cohorts and get the all the users
    df = df.groupby(["day", "user"]).sum().reset_index()
    df = df[["day", "user"]]

    infra.pd.clean_write_parquet(df.compute(), outfile)


def make_plot(infile):
    early_users = infra.pd.read_parquet("data/clean/initial_user_balances_INDEX_none.parquet")
    registered_users = early_users.assign(timestamp=infra.constants.MIN_DATE)

    transactions = infra.pd.read_parquet("data/clean/transactions_DIV_none_INDEX_timestamp.parquet").reset_index()

    registered_users = registered_users.append(transactions).sort_values("timestamp").groupby("user").first()
    registered_users = registered_users.reset_index().sort_values("timestamp").reset_index()
    registered_users = registered_users.assign(temp=1)
    registered_users["count"] = registered_users["temp"].cumsum()
    registered_users = registered_users.drop(["temp", "user"], axis="columns").rename(columns={"count": "user"})
    registered_users["day"] = registered_users["timestamp"].dt.floor("d")

    # Generate a dense dataframe with all days
    date_range = pd.DataFrame({"day": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="1D")})
    registered_users = date_range.merge(
        registered_users,
        how="left",
        left_on="day",
        right_on="day",
    ).fillna(method="ffill").dropna()

    user_days = infra.pd.read_parquet(infile)

    active_users = user_days.groupby("day")["user"].nunique()
    active_users = active_users.to_frame().reset_index()

    # Group weekly to capture the total number of unique users across the entire week and account for intermittent use.
    weekly_users = user_days.groupby(pd.Grouper(key="day", freq="W-MON"))["user"].nunique()
    weekly_users = weekly_users.to_frame().reset_index().rename(columns={"user": "week_unique_users"})
    week_range = pd.DataFrame({"day": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="W-MON")})
    weekly_users = weekly_users.merge(week_range, on="day", how="outer")
    weekly_users.fillna(0)

    monthly_users = user_days.groupby(pd.Grouper(key="day", freq="M"))["user"].nunique()
    monthly_users = monthly_users.to_frame().reset_index().rename(columns={"user": "month_unique_users"})
    month_range = pd.DataFrame({"day": pd.date_range(infra.constants.MIN_DATE, infra.constants.MAX_DATE, freq="M")})
    monthly_users = monthly_users.merge(month_range, on="day", how="outer")
    monthly_users = monthly_users.fillna(0)

    # Join the active and registered users together
    users = active_users.merge(registered_users,
                               how="right",
                               left_on="day",
                               right_on="day",
                               suffixes=('_active', '_registered'))
    users = users.merge(weekly_users, how="outer", on="day")
    users = users.merge(monthly_users, how="outer", on="day")

    # For cohorts with no active users, fill zero.
    users["user_active"] = users["user_active"].fillna(value=0)

    users = users.rename(columns={"day": "date", "user_active": "Unique Daily Online", "user_registered": "Registered", "week_unique_users": "Unique Weekly Online", "month_unique_users": "Unique Monthly Online"})
    users = users.set_index("date").sort_index()
    users["Registered"] = users["Registered"].fillna(method="ffill")
    users["Unique Weekly Online"] = users["Unique Weekly Online"].fillna(method="bfill")
    users["Unique Monthly Online"] = users["Unique Monthly Online"].fillna(method="bfill")
    users = users.reset_index()

    # Limit graphs to the study period
    users = users.loc[users["date"] < infra.constants.MAX_DATE]

    # Compute a rolling average
    users["Active 7-Day Average"] = users["Unique Daily Online"].rolling(
        window=7,
    ).mean()

    # Get the data in a form that is easily plottable
    users = users.melt(id_vars=["date"], value_vars=["Registered", "Unique Monthly Online", "Unique Weekly Online", "Unique Daily Online"], var_name="user_type", value_name="num_users")
    # Drop the rolling average... it wasn't useful
    # users = users.melt(id_vars=["date"], value_vars=["Active", "Registered", "Active 7-Day Average", "Unique Weekly Active"], var_name="user_type", value_name="num_users")
    # Reset the types of the dataframe
    types = {
        "date": "datetime64",
        "num_users": "int64"
    }
    # Required since some rolling average entries are NaN before the average window is filled.
    users = users.dropna()
    users = users.astype(types)

    users = users.sort_values(["date", "num_users"])
    label_order = {
        "Registered": 1,
        "Unique Monthly Online": 2,
        "Unique Weekly Online": 3,
        "Unique Daily Online": 4,
    }
    # Mergesort is stablely implemented : )
    users = users.sort_values(
        ["user_type"],
        key=lambda col: col.map(lambda x: label_order[x]),
        kind="mergesort",
    )
    users = users.reset_index()

    altair.Chart(users).mark_line(interpolate='step-after').encode(
        x=altair.X("date:T",
                   title="Time",
                   axis=altair.Axis(
                       labelSeparation=5,
                       labelOverlap="parity",
                   ),
                   ),
        y=altair.Y("num_users",
                   title="User Count",
                   ),
        color=altair.Color(
            "user_type",
            title="",
            sort=None,
            legend=altair.Legend(
                orient="top-left",
                fillColor="white",
                labelLimit=500,
                padding=10,
                strokeColor="black",
            ),
        ),
        strokeDash=altair.StrokeDash(
            "user_type",
            sort=None,
        ),
    ).properties(width=500).save("renders/users_per_week.png", scale_factor=2)


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temporary_file = "scratch/graphs/users_per_week"
    if platform.large_compute_support:
        print("Running compute tasks")
        print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
        client = infra.dask.setup_platform_tuned_dask_client(10, platform)
        reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
        client.close()

    if platform.altair_support:
        print("Running vis tasks")
        make_plot(graph_temporary_file)

    print("Done!")
