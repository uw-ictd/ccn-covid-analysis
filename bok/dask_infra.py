"""Sets up dask for local compute
"""

import dask.config
import dask.distributed
import dask.dataframe
import shutil


def setup_dask_client():
    """Setup and configure a dask client for the local system
    """

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
    # ------------------------------------------------
    # Dask tuning, currently set for a 8GB RAM laptop
    # ------------------------------------------------
    cluster = dask.distributed.LocalCluster(n_workers=3,
                                            threads_per_worker=1,
                                            memory_limit='3GB')

    return dask.distributed.Client(cluster)


def clean_write_parquet(dataframe, path, engine="fastparquet", compute=True):
    """Write a parquet file with common options standardized in the project
    """

    # Ensure we are writing dask dataframes, not accidentally pandas!
    if not isinstance(dask.dataframe.DataFrame, dataframe):
        raise ValueError(
            "Attempted to write dask dataframe, but got a {}".format(
                str(type(dataframe))
            )
        )

    # Clear the dest directory to prevent partial mixing of files from an old
    # archive if the number of partitions has changed.
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass

    # Do the write with the given options.
    return dataframe.to_parquet(path,
                                compression="snappy",
                                engine=engine,
                                compute=compute)


def read_parquet(path):
    """Read a parquet file with common options standardized in the project
    """
    return dask.dataframe.read_parquet(path, engine="fastparquet")
