""" Computing active and registered users on the network over time
"""

import altair
import bok.dask_infra
import dask.dataframe
import dask.distributed


def get_sites_visited_query(data):
    # Get the date column back
    query = data.reset_index()
    # Group by the number of times each site was visited
    query = query.groupby("category").count()
    # Get the category column back
    query = query.reset_index()
    # Create a frequency column
    query["frequency"] = query["user"]
    return query


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.

    data = dask.dataframe.read_parquet("data/clean/typical_with_fqdn_category", engine="pyarrow")
    length = len(data)
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    print("Processing {} flows".format(length))

    sites_visited = get_sites_visited_query(data)
    sites_visited = sites_visited.melt(id_vars=["category"], value_vars=["frequency"], var_name="website", value_name="frequency")
    sites_visited = sites_visited.compute()

    altair.Chart(sites_visited).mark_bar().encode(
        x="category",
        y="frequency",
    ).serve()

