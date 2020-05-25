import altair as alt
import pandas as pd
import dask.dataframe
import dask.distributed


import bok.parsers
import bok.dask_infra


def compute_user_currency_histories():
    """Compute the normalized ledger with running user currency balance.
    """
    # Extract data from the transactions file into a resolved pandas frame
    transactions = dask.dataframe.read_parquet("data/clean/transactions",
                                               engine="fastparquet").compute()

    # Split transfers into positive components for the dest and negative for
    # the source
    transfers = transactions.loc[
        (transactions["kind"] == "user_transfer") |
        (transactions["kind"] == "admin_transfer")
        ].reset_index().drop(["index", "amount_bytes", "kind"], axis="columns")

    user_ledger = transfers[["timestamp", "dest_user", "amount_idr"]]
    user_ledger = user_ledger.rename({"dest_user": "user"}, axis="columns")

    temp_ledger = transfers[["timestamp", "user", "amount_idr"]]
    temp_ledger["amount_idr"] = -temp_ledger["amount_idr"]

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

    return running_user_balance


if __name__ == "__main__":
    running_user_balances = compute_user_currency_histories()

    test = running_user_balances.loc[
        running_user_balances["user"] ==
        "5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb"]
    # aaa16576d20325cbb47b9fc03d431f0728e51265aa2369385ea060e1b5524988
    # ff26563a118d01972ef7ac443b65a562d7f19cab327a0115f5c42660c58ce2b8
    # 5759d99492dc4aace702a0d340eef1d605ba0da32a526667149ba059305a4ccb

    test = test.reset_index().sort_values("timestamp")
    pd.set_option('display.max_rows', None)

    alt.Chart(test).mark_line(interpolate='step-after').encode(
        x="timestamp",
        y="balance",
        tooltip=["balance", "amount_idr"],
    ).show()
