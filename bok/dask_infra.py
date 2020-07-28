"""Sets up dask for local compute
"""

import dask.config
import dask.distributed
import dask.dataframe
import os
import shutil


def setup_dask_client():
    """Setup and configure a dask client for the local system
    """
    return setup_tuned_dask_client(1, 3, 4)


def setup_tuned_dask_client(per_worker_memory_GB, system_memory_GB, system_processors):
    # Compression sounds nice, but results in spikes on decompression
    # that can lead to unstable RAM use and overflow.
    dask.config.set({"dataframe.shuffle-compression": False})
    dask.config.set({"distributed.scheduler.allowed-failures": 50})
    dask.config.set({"distributed.scheduler.work-stealing": True})

    if per_worker_memory_GB >= system_memory_GB:
        # We are operating on a constrained system
        memory_limit = "{}GB".format(system_memory_GB)
        print("Operating constrained with {} processors, {} threads, and {} memory".format(
            1, system_processors, memory_limit
        ))

        # The memory limit parameter is undocumented and applies to each worker.
        cluster = dask.distributed.LocalCluster(n_workers=1,
                                                threads_per_worker=system_processors,
                                                memory_limit=memory_limit)

        # Aggressively write to disk but don't kill worker processes if
        # they stray. With a small number of workers each worker killed is
        # big loss. The OOM killer will take care of the overall system.
        dask.config.set({"distributed.worker.memory.target": 0.3})
        dask.config.set({"distributed.worker.memory.spill": 0.4})
        dask.config.set({"distributed.worker.memory.pause": 0.6})
        dask.config.set({"distributed.worker.memory.terminate": False})
    else:
        # Run as many workers available with the required amount of memory per worker
        worker_count = int(system_memory_GB/per_worker_memory_GB)
        if worker_count <= 0:
            raise ValueError("tuning operation failure")

        worker_count = min(system_processors, worker_count)

        # Allocate extra processors to threads
        thread_count = int(system_processors/worker_count)
        print("Operating with {} workers, {} threads per worker, and {} memory".format(
            worker_count, thread_count, per_worker_memory_GB
        ))
        # The memory limit parameter is undocumented and applies to each worker.
        cluster = dask.distributed.LocalCluster(n_workers=worker_count,
                                                threads_per_worker=thread_count,
                                                memory_limit=per_worker_memory_GB)

        # Less aggressively write to disk than in the constrained case,
        # but still don't kill worker processes if they stray and rely on the
        # OOM killer if there is a runaway process.
        dask.config.set({"distributed.worker.memory.target": 0.5})
        dask.config.set({"distributed.worker.memory.spill": 0.8})
        dask.config.set({"distributed.worker.memory.pause": 0.9})
        dask.config.set({"distributed.worker.memory.terminate": False})

    return dask.distributed.Client(cluster)


def clean_write_parquet(dataframe, path, engine="fastparquet", compute=True):
    """Write a parquet file with common options standardized in the project
    """

    # Ensure we are writing dask dataframes, not accidentally pandas!
    if not isinstance(dataframe, dask.dataframe.DataFrame):
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


def merge_parquet_frames(in_parent_directory, out_frame_path, index_column):
    """Iterate through divs in a parent directory and merge to the out frame
    """
    merged_frame = None
    div_on_disk = sorted(os.listdir(in_parent_directory))
    for div in div_on_disk:
        div_path = os.path.join(in_parent_directory, div)
        frame = read_parquet(div_path)

        if merged_frame is None:
            merged_frame = frame
        else:
            merged_frame = merged_frame.append(frame)

    merged_frame = merged_frame.reset_index().set_index(
        index_column
    ).repartition(
        partition_size="64M",
        force=True
    )

    clean_write_parquet(merged_frame, out_frame_path)
