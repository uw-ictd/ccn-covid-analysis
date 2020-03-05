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
    # Create a frequency column
    query["frequency"] = query["user"]
    return query


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()

    # Import the dataset
    #
    # Importantly, dask is lazy and doesn't actually import the whole thing,
    # but just keeps track of where the file shards live on disk.
    data = dask.dataframe.read_parquet("data/clean/flows/typical_with_fqdn_category", engine="fastparquet")
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")
    # length = len(data)
    # print("Processing {} flows".format(length))

    sites_visited = get_sites_visited_query(data)

    # Checkpoint the intermediate groupby product.
    bok.dask_infra.clean_write_parquet(sites_visited, "scratch/sites_visited")
    sites_visited = dask.dataframe.read_parquet("scratch/sites_visited", engine="fastparquet")

    # Convert to pandas!
    # This should be quick since the grouby has already happened.
    sites_visited = sites_visited.compute()
    # Get the category column back
    sites_visited = sites_visited.reset_index()
    sites_visited = sites_visited.melt(id_vars=["category"], value_vars=["frequency"], var_name="website", value_name="frequency")

    altair.Chart(sites_visited).mark_bar().encode(
        x="category",
        y="frequency",
    ).serve()

