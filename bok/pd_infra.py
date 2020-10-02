"""Methods pandas manipulations with shared parameters across the project
"""

import pandas as pd
import os


def clean_write_parquet(dataframe, path):
    """Write a parquet file with common options standardized in the project
    """

    # Ensure we are writing pandas dataframes, not accidentally dask!
    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError(
            "Attempted to write dask dataframe, but got a {}".format(
                str(type(dataframe))
            )
        )

    # Clear the dest directory to prevent partial mixing of files from an old
    # archive if the number of partitions has changed.
    try:
        os.remove(path)
    except FileNotFoundError:
        # No worries if the output doesn't exist yet.
        pass

    # Do the write with the given options.
    return dataframe.to_parquet(path,
                                compression="snappy",
                                engine="fastparquet")


def read_parquet(path):
    """Read a parquet file with common options standardized in the project
    """
    return pd.read_parquet(path, engine="fastparquet")


def cartesian_product(left, right):
    """Generate a cartesian product of left/right at O(N*M) size!"""
    temp_left = left.assign(temp_key=1)
    temp_right = right.assign(temp_key=1)
    return temp_left.merge(temp_right, on="temp_key").drop("temp_key", axis=1)
