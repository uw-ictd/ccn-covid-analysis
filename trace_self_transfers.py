import altair as alt
import collections
import datetime
import pandas as pd

import bok.constants
import bok.pd_infra
import bok.parsers


# Module specific format options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 40)


def remove_nuls_from_file(source_path, dest_path):
    with open(source_path, mode='rb') as sourcefile:
        data = sourcefile.read()
        nul_count = data.count(b'\x00')
        if nul_count > 0:
            print("{} nul values found-- file is corrupted".format(nul_count))
            _print_nuls_with_context(data)

        # Remove nuls from the file.
        with open(dest_path, mode='w+b') as destfile:
            destfile.write(data.replace(b'\x00', b''))


def _print_nuls_with_context(binary_stream):
    """Print the context around runs of nul values"""
    start_run_index = None
    for index, byte in enumerate(binary_stream):
        if byte == 0x00:
            if start_run_index is None:
                start_run_index = index
        else:
            if start_run_index is not None:
                nul_count = index - start_run_index
                print("nul from [{}, {})".format(start_run_index, index))
                print(repr(binary_stream[start_run_index-30:start_run_index]) +
                      "{}X".format(nul_count) + '*NUL* ' +
                      repr(binary_stream[index:index+30]))
                start_run_index = None


def _run_clean():
    remove_nuls_from_file("data/transaction-debug/raw.log",
                          "data/transaction-debug/null-free.log")

    transactions = bok.parsers.parse_transactions_log(
        "data/transaction-debug/null-free.log")

    bok.pd_infra.clean_write_parquet(transactions,
                                     "data/transaction-debug/cleaned.parquet")


def _make_charts():
    transactions = bok.pd_infra.read_parquet("data/transaction-debug/cleaned.parquet")
    transfers = transactions.loc[transactions["kind"] == "user_transfer"]
    self_transfers = transfers.loc[transfers["user"] == transfers["dest_user"]]

    # Make an overall chart
    alt.Chart(self_transfers).mark_rule(strokeWidth=2, opacity=0.5).encode(
        x=alt.X('timestamp:T',
                ),
        color=alt.Color(
            "user",
            legend=None,
        ),
        tooltip=alt.Tooltip(["amount_idr", "user"]),
    ).properties(
        width=1000,
        height=500,
    ).save("renders/self-transfers-vs-time.png")


    early_self_transfers = self_transfers.loc[self_transfers["timestamp"] < datetime.datetime(year=2020, month=5, day=24)]
    late_self_transfers = self_transfers.loc[self_transfers["timestamp"] >= datetime.datetime(year=2020, month=5, day=24)]

    print("The following users made an accidental self-tranfser but did not "
          "explot the bug. No further self transfers were recorded from these "
          "users.")
    print(early_self_transfers)

    print("These transfers occurred later, and appeared under active exploit after UTC 2020, May 24")
    late_self_transfers = late_self_transfers.assign(count=1)
    grouped_transfers = late_self_transfers.groupby("user").sum()
    print(grouped_transfers)
    exploiters = grouped_transfers.index.unique()

    print("These users received a transfer from one of the implicated users after UTC 2020, May 24")
    late_transfers = transfers.loc[transfers["timestamp"] >= datetime.datetime(year=2020, month=5, day=24)]
    indirect_beneficiaries = late_transfers.loc[
        ((late_transfers["user"] != late_transfers["dest_user"]) &
         (late_transfers["user"].isin(exploiters)))]
    print(indirect_beneficiaries)
    exploiters_set = set(exploiters)
    indirect_beneficiaries_set = set(indirect_beneficiaries["user"].unique())

    print(exploiters_set)
    print(indirect_beneficiaries_set)
    print("Willing to use but not do:")
    print(indirect_beneficiaries_set - exploiters_set)

    print("Compute per-user tainted credit amount and tainted data amount")
    suspect_users = exploiters_set.union(indirect_beneficiaries_set)
    possible_suspect_transactions = transactions.loc[((transactions["timestamp"] >= datetime.datetime(year=2020, month=5, day=24)) &
                                                      (transactions["user"].isin(suspect_users) | transactions["dest_user"].isin(suspect_users)))]

    user_credit_taint=collections.defaultdict(int)
    user_data_taint=collections.defaultdict(int)
    for txn in possible_suspect_transactions.itertuples():
        if txn.kind == "purchase":
            if user_credit_taint[txn.user] - txn.amount_idr > 0:
                user_credit_taint[txn.user] -= txn.amount_idr
                user_data_taint[txn.user] += txn.amount_bytes
        elif txn.kind == "user_transfer":
            if txn.user == txn.dest_user:
                user_credit_taint[txn.user] += txn.amount_idr
            else:
                if user_credit_taint[txn.user] > 0:
                    user_credit_taint[txn.user] -= txn.amount_idr
                    user_credit_taint[txn.dest_user] += txn.amount_idr
        else:
            print(txn)

    print(user_credit_taint)
    print(user_data_taint)

    # Construct the taint dataframe
    credit = list()
    data = list()
    users = list()
    for key in user_credit_taint.keys():
        users.append(key)
        data.append(user_data_taint[key])
        credit.append(user_credit_taint[key])

    taint = pd.DataFrame({"user": users, "amount_bytes": data, "amount_idr": credit})
    taint = taint.astype({"amount_bytes": int, "amount_idr": int})

    taint["amount_bytes"] = taint["amount_bytes"]
    taint["amount_MB"] = taint["amount_bytes"] / 1000**2
    print(taint)

    melted = taint.melt(
        id_vars=["user"],
        value_vars=["amount_bytes", "amount_idr"],
        var_name="type",
        value_name="amount",
    )
    print(melted)
    alt.Chart(melted).mark_bar().encode(
        x=alt.X(
            "user",
        ),
        y=alt.Y(
            "amount",
            #scale=alt.Scale(type="log")
        ),
        color="type"
    ).properties(
        width=500,
        height=500,
    ).facet(
        column=alt.Column(
            'type:N',
            title="",
        ),
    ).resolve_scale(
        y="independent",
    ).show()

if __name__ == "__main__":
    # _run_clean()
    _make_charts()
