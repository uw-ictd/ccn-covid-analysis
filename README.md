# Bokondini Data Analysis

This analysis is a continuation of excellent work begun by [Michelle
Lin](https://github.com/IntOwl).

## Data

Data for this analysis can be requested from the UW ICTD lab and
shared for research purposes on a valid case-by-case basis. See the
data specific [data-readme](data/README.md) for specific information.

## Running Computations

### Dependencies

Dependencies for the project are managed with Pipenv. After checking
out the repository, run `pipenv shell` in the root directory to get a
shell in a virtual environment. Then run `pipenv sync` to install all
the dependencies from the lockfile. If this command gives you errors,
you may need to install some supporting libraries (libsnappy) for your
platform. You will need to be in the pipenv shell or run commands with
pipenv run to actually run scripts with the appropriate dependencies.

### Dask
The uncompressed dataset size as of Feb 2020 is too large to fit on
one reasonable sized machine (~70M rows / 29GB just for flows). For
now the project uses [dask](https://docs.dask.org/en/latest/), a
framework for python that extends pandas data frames to support
writing intermediates to disk or running on a large distributed
cluster. See some of the existing computation files for quick examples
of how to tune dask for your local machine.

### Intermediates
Running big tasks via dask over the entire dataset can be slow, so I
would advise creating intermediate datasets if needed. See the
`canonicalize_data.py` module for some examples of storing
intermediate datasets to parquet, a pretty fast to read from disk
format.

## Lessons Learned

I spent a while fighting with dask and learned a couple of important
tidbits:

1. Dask is pretty fast moving. Tracking the latest tag resulted in
   picking up a memory leak bug and some other small issues. I've
   moved back to tracking a specific version. We can update
   incrementally as needed, but it should be a conscious decision.

2. The Dask API documentation is not up to date. They expose some
   functions as extensions from Pandas, but Pandas is a moving
   target. The API available in "passthrough" functions to the
   underlying pandas dataframe __will depend on the pandas version__
   not the dask version. Check the pandas documentation for the most
   up to date options.

3. Reasonable size divisions (or partitions-- called the same thing at
   different points in the docs) are important (the entire division
   should fit in RAM with room to spare for computational
   intermediates and overhead). Some calls or options to calls will
   sneakily repartition your dataflow and lead to bad performance with
   limited ram. One of these options is the interleave_partitions flag
   to dataframe.multi.concat (or dataframeinstance.append)...

   * If you accidentally generate a large division, you won't be
     warned about this, and the number of partitions in the dataframe
     could actually remain the same. I have not been able to find a
     good API for getting the number of rows in each division. (you
     can loop over each partition with get_partition(i)).  With a
     large division dask will struggle through, but some API calls
     (particularly to_parquet) will barf with high memory usage. The
     offending call which created the poor partitioning could be much
     earlier in the call chain!

4. "Indexes" in dask/pandas are kind of odd. __Unlike SQL__ making
   something an index removes the column from the table and moves the
   column into a side "index" structure (`dataframeInstance.index` to
   access). There is a new pandas option to not drop the column from
   the table, but __don't use it for now__, since some specialized
   dask functions get confused (looking at you to_parquet) and will
   error on a duplicate column name.

   * Even more odd, calling set index again replaces the current index
     with a new column but does not re-add the column that was indexed
     back into the dataset (aka drops it completely!). You can re-add
     it manually though with the dictionary dataframe interface before
     setting the new index (see canonicalize_data.py).
