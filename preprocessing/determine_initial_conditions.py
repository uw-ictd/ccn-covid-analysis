""" Determine the balances at the beginning of the dataset"""

import datetime
import pandas as pd

from infra.constants import MIN_DATE
import infra.pd
import infra.platform


def compute_initial_conditions(untrimmed_file, initial_conditions_outfile):
    """Compute the normalized ledger with running user currency balance.
    """
    # Extract data from the transactions file into a resolved pandas frame
    # Importantly, use the timezone adjusted log but NOT the trimmed log to
    # avoid clipping state from early users.
    transactions = infra.pd.read_parquet(untrimmed_file)

    # Limit normalization to before 2020 to avoid the self-transfer anomaly
    transactions = transactions.loc[transactions["timestamp"] < datetime.datetime.strptime('2020-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')]

    # Split transfers into positive components for the dest and negative for
    # the source
    transfers = transactions.loc[
        (transactions["kind"] == "user_transfer") |
        (transactions["kind"] == "admin_transfer")
        ].reset_index().drop(["index", "amount_bytes", "kind"], axis="columns")

    user_ledger = transfers[["timestamp", "dest_user", "amount_idr"]]
    user_ledger = user_ledger.rename({"dest_user": "user"}, axis="columns")

    temp_ledger = transfers.loc[:, ["timestamp", "user", "amount_idr"]]
    temp_ledger["amount_idr"] = temp_ledger["amount_idr"] * -1

    user_ledger = user_ledger.append(temp_ledger).reset_index().drop("index", axis="columns")

    # Add topups from the admin as positive user balance
    topups = transactions.loc[
        (transactions["kind"] == "admin_topup")
    ].reset_index().drop(
        ["index", "amount_bytes", "user", "kind"], axis="columns"
    )

    topups = topups.rename({"dest_user": "user"}, axis="columns")

    user_ledger = user_ledger.append(topups).reset_index().drop("index", axis="columns")

    # Add purchases as negative balance
    purchases = transactions.loc[
        (transactions["kind"] == "purchase")
    ].reset_index().drop(["index", "amount_bytes", "dest_user", "kind"], axis="columns")

    purchases["amount_idr"] = -purchases["amount_idr"]

    user_ledger = user_ledger.append(purchases).set_index("timestamp")

    running_user_balance = user_ledger
    running_user_balance = running_user_balance.sort_values("timestamp")
    running_user_balance["balance"] = running_user_balance.groupby("user")["amount_idr"].transform(pd.Series.cumsum)

    # Account for offsets from before logging was added.
    negative_balances = running_user_balance.loc[running_user_balance["balance"] < 0, ["user", "balance"]]
    print("==================================================")
    print("Correcting for negative user balance with {} users".format(
        len(negative_balances["user"].unique())
    ))
    print("==================================================")

    negative_offsets = negative_balances.groupby("user").min().rename(columns={"balance": "offset"})
    negative_offsets["offset"] = -negative_offsets["offset"]
    negative_offsets["inferred"] = True

    # Determine sum up to min date
    early_transactions = running_user_balance.loc[running_user_balance.index < MIN_DATE]

    # Add in pre-logging offsets inferred by users going negative
    early_transaction_totals = early_transactions.groupby("user").sum().reset_index().drop("balance", axis="columns")
    early_transaction_totals = early_transaction_totals.merge(negative_offsets, on="user", how="outer")
    early_transaction_totals["inferred"] = early_transaction_totals["inferred"].fillna(False)
    early_transaction_totals = early_transaction_totals.fillna(0)
    early_transaction_totals["amount_idr"] += early_transaction_totals["offset"]

    # Cleanup the dataframe series and sort users
    early_transaction_totals = early_transaction_totals.drop(["offset"], axis="columns").set_index("user").sort_index().reset_index()
    early_transaction_totals = early_transaction_totals.rename({"amount_idr": "balance_idr"}, axis="columns")

    infra.pd.clean_write_parquet(early_transaction_totals, initial_conditions_outfile)


if __name__ == "__main__":
    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    compute_initial_conditions(
        untrimmed_file="scratch/transactions_TZ.parquet",
        initial_conditions_outfile="scratch/initial_user_balances.parquet"
    )
