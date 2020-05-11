""" An example to show how to compute values over the whole dataset with dask
"""

import altair
import bok.dask_infra
import dask.dataframe
import dask.distributed
import numpy as np
import pandas as pd

if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet(
        "data/clean/flows/typical_DIV_none_INDEX_user", engine="fastparquet")

    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    print("Processing {} flows".format(len(flows)))
    # The user column will become an index of the returned grouped frame.
    user_totals = flows.groupby("user").aggregate({"bytes_up": np.sum,
                                                   "bytes_down": np.sum})

    # Calling compute will realize user_totals as a single local pandas
    # dataframe. Be careful calling .compute() on big stuff! You may run out
    # of memory...
    user_totals = user_totals.compute()

    # Once you have a total, you might want to dump it to disk so you don't
    # have to re-run the dask computations again! At this point you can work
    # in a jupyter notebook from the intermediate file if that's easier too.
    # user_totals.to_parquet("scratch/temp",
    #                        compression="snappy",
    #                        engine="fastparquet")
    
    # user_totals = dask.dataframe.read_parquet("scratch/temp",
    #                                           engine="fastparquet").compute()

    # Once user_totals has been computed and you can do things with
    # it as a normal dataframe : )
    print(user_totals)
    print("user_totals is a ", type(user_totals))

    # Add the index back as a dataframe column
    user_totals = user_totals.reset_index()

    # Transform to long form for altair.
    # https://altair-viz.github.io/user_guide/data.html#converting-between-long-form-and-wide-form-pandas
    user_totals = user_totals.melt(id_vars=["user"],
                                   value_vars=["bytes_up", "bytes_down"],
                                   var_name="direction",
                                   value_name="amount",
                                   )
    print(user_totals)
    print("Check out a basic chart!")
    chart = altair.Chart(user_totals).mark_bar().encode(
        x=altair.X("direction", title="", axis=altair.Axis(labels=False)),
        y=altair.Y("amount:Q", title="Amount (Bytes)"),
        color="direction",
        column=altair.Column('user',
                             title="",
                             header=altair.Header(labelOrient="bottom",
                                                  labelAngle=-60,
                                                  labelAnchor="middle",
                                                  labelAlign="right",
                                                  ),
                             sort=altair.SortField(field="amount",
                                                   order="descending"
                                                   ),
                            ),
    ).properties(
        title="Data used per user"
    ).configure_title(
        fontSize=20,
        font='Courier',
        anchor='start',
        color='gray'
    ).serve(port=8891, open_browser=False)
