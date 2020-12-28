import datetime
import pandas as pd
import altair as alt

import infra.constants
import infra.dask
import infra.pd


def generate_consolidated_purchases(outfile):
    transactions = infra.pd.read_parquet(
        "data/clean/transactions_DIV_none_INDEX_timestamp.parquet")

    # Consolidate together closely spaced small purchases by iterating
    # through each user's history.
    purchases = transactions.loc[transactions["kind"] == "purchase"]
    users = purchases["user"].unique()
    consolidated_purchases = []
    for user in users:
        current_user_purchases = purchases.loc[purchases["user"] == user]
        current_user_purchases = current_user_purchases.sort_values(
            "timestamp"
        )

        # Manually iterate since rolling for datetimes is not implemented.
        last_purchase_time = None
        consolidated_user_purchases = []
        in_progress_purchase = None

        for row in current_user_purchases.itertuples():
            if last_purchase_time is None:
                # Startup on first iteration
                in_progress_purchase = _purchase_tuple_to_dict(row)
                in_progress_purchase["time_since_last_purchase"] = None
                last_purchase_time = row.timestamp
                continue

            if (row.timestamp - last_purchase_time < datetime.timedelta(seconds=60)):
                in_progress_purchase["amount_bytes"] += row.amount_bytes
                in_progress_purchase["amount_idr"] += row.amount_idr
            else:
                consolidated_user_purchases.append(in_progress_purchase)
                prior_consolidated_timestamp = in_progress_purchase["timestamp"]
                in_progress_purchase = _purchase_tuple_to_dict(row)
                in_progress_purchase["time_since_last_purchase"] = row.timestamp - prior_consolidated_timestamp

            # Always increment the last purchase time to the current row time
            last_purchase_time = row.timestamp

        # Cleanup remaining dangling purchase if one exists
        if in_progress_purchase is not None:
            consolidated_user_purchases.append(in_progress_purchase)

        consolidated_purchases += consolidated_user_purchases

    consolidated_purchases_frame = pd.DataFrame(consolidated_purchases)
    infra.pd.clean_write_parquet(consolidated_purchases_frame, outfile)


def _purchase_tuple_to_dict(tuple):
    return {'timestamp': tuple.timestamp,
            'kind': tuple.kind,
            'user': tuple.user,
            'amount_bytes': tuple.amount_bytes,
            'amount_idr': tuple.amount_idr
            }


def make_plot(infile):
    purchases = infra.pd.read_parquet(infile)

    # Drop nulls from the first purchase
    clean_purchases = purchases.dropna()
    # Convert timedelta to seconds for altair compatibility
    clean_purchases["time_since_last_purchase"] = clean_purchases["time_since_last_purchase"].transform(pd.Timedelta.total_seconds)
    clean_purchases = clean_purchases[["user", "time_since_last_purchase", "amount_bytes"]]

    aggregate = clean_purchases.groupby(["user"]).agg({"time_since_last_purchase": ["mean", lambda x: x.quantile(0.90), lambda x: x.quantile(0.99)]})
    # Flatten column names
    aggregate = aggregate.reset_index()
    aggregate.columns = [' '.join(col).strip() for col in aggregate.columns.values]
    aggregate = aggregate.rename(
        columns={"time_since_last_purchase mean": "mean",
                 "time_since_last_purchase <lambda_0>": "q90",
                 "time_since_last_purchase <lambda_1>": "q99",
                 })

    # Compute a CDF since the specific user does not matter
    stats_mean = compute_cdf(aggregate, "mean", "user")
    stats_mean = stats_mean.rename(columns={"mean": "value"})
    stats_mean["type"] = "User's Mean"

    stats_q90 = compute_cdf(aggregate, "q90", "user")
    stats_q90 = stats_q90.rename(columns={"q90": "value"})
    stats_q90["type"] = "User's 90% Quantile"

    stats_q99 = compute_cdf(aggregate, "q99", "user")
    stats_q99 = stats_q99.rename(columns={"q99": "value"})
    stats_q99["type"] = "User's 99% Quantile"

    stats_frame = stats_mean.append(stats_q90).append(stats_q99)

    # Convert to Days
    stats_frame["value"] = stats_frame["value"] / 86400
    print(stats_frame)

    alt.Chart(stats_frame).mark_line(clip=True).encode(
        x=alt.X('value:Q',
                scale=alt.Scale(type="log", domain=(0.1, 80)),
                title="Time Between Purchases (Hours) (Log Scale)"
                ),
        y=alt.Y('cdf',
                title="Fraction of Users (CDF)",
                scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
        color=alt.Color(
            "type",
            sort=None,
            legend=alt.Legend(
                title="",
                orient="bottom-right",
                fillColor="white",
                labelLimit=500,
                padding=5,
                strokeColor="black",
                columns=1,
            ),
        ),
        strokeDash=alt.StrokeDash(
            "type",
            sort=None,
        )
    ).properties(
        width=500,
        height=200,
    ).save("renders/purchase_timing_per_user_cdf.png", scale_factor=2.0)

def make_amount_plot(infile):
    purchases = infra.pd.read_parquet(infile)
    purchases = purchases.assign(count=1)
    purchases = purchases[["amount_bytes", "count"]].groupby(["amount_bytes"]).sum().reset_index()
    purchases["amount_mb"] = purchases["amount_bytes"] / 1000**2

    alt.Chart(purchases).mark_point().encode(
        x=alt.X(
            "amount_mb",
            title="Session Purchase Amount (MB) (Log Scale)",
            scale=alt.Scale(
                type="log",
                domain=(10, 2000)
            ),
        ),
        y=alt.Y(
            "count",
            title="Occurrences (Count) (Log Scale)",
            scale=alt.Scale(
                type="log",
            ),
        ),
        color=alt.condition(
            'datum.count>1000',
            alt.ColorValue('red'), alt.ColorValue('steelblue'),
        ),
        shape=alt.condition(
            'datum.count>1000',
            alt.ShapeValue('diamond'), alt.ShapeValue('triangle'),
        ),
    ).properties(
        width=500,
        height=300,
    ).save("renders/purchase_timing_per_user_clumped_amounts.png", scale_factor=2.0)


    print(purchases)


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    graph_temporary_file = "scratch/graphs/purchase_timing_per_user"
    generate_consolidated_purchases(outfile=graph_temporary_file)
    make_plot(graph_temporary_file)
    make_amount_plot(graph_temporary_file)
