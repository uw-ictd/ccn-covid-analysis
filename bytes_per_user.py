""" An example to show how to compute values over the whole dataset with dask
"""

import altair
import dask.config
import dask.dataframe
import dask.distributed
import numpy as np
import pandas as pd

if __name__ == "__main__":
    # ------------------------------------------------
    # Dask tuning, currently set for a 16GB RAM laptop
    # ------------------------------------------------

    # Compression sounds nice, but results in spikes on decompression
    # that can lead to unstable RAM use and overflow.
    dask.config.set({"dataframe.shuffle-compression": False})
    dask.config.set({"distributed.scheduler.allowed-failures": 50})
    dask.config.set({"distributed.scheduler.work-stealing": True})

    # Aggressively write to disk but don't kill worker processes if
    # they stray. With a small number of workers each worker killed is
    # big loss. The OOM killer will take care of the overall system.
    dask.config.set({"distributed.worker.memory.target": 0.2})
    dask.config.set({"distributed.worker.memory.spill": 0.4})
    dask.config.set({"distributed.worker.memory.pause": 0.6})
    dask.config.set({"distributed.worker.memory.terminate": False})

    # The memory limit parameter is undocumented and applies to each worker.
    cluster = dask.distributed.LocalCluster(n_workers=2,
                                            threads_per_worker=1,
                                            memory_limit='4GB')
    client = dask.distributed.Client(cluster)

    # Import the flows dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    flows = dask.dataframe.read_parquet("data/clean/flows", engine="pyarrow")
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
    #                        engine="pyarrow")

    # Once user_totals has been computed and you can do things with
    # it as a normal dataframe : )
    # print(user_totals)
    # print("user_totals is a ", type(user_totals))

    # Add the index back as a dataframe column
    user_totals["user"] = user_totals.index

    # Transform to long form for altair.
    # https://altair-viz.github.io/user_guide/data.html#converting-between-long-form-and-wide-form-pandas
    user_totals = user_totals.melt(id_vars=["user"],
                                   value_vars=["bytes_up", "bytes_down"],
                                   var_name="direction",
                                   value_name="amount",
                                   )
    print(user_totals)
    print("Check out a basic chart!")
    altair.Chart(user_totals).mark_bar().encode(
        x="direction",
        y="amount:Q",
        color="direction",
        column=altair.Column('user',
                             title="User",
                             header=altair.Header(labelAngle=60,labelOrient="top")
                            ),
    ).serve()
