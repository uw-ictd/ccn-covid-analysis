""" Computing active and registered users on the network over time
"""

import altair as alt
import pandas as pd

import bok.dask_infra
import bok.pd_infra


def reduce_to_pandas(outfile, dask_client):
    flows = bok.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start")[["category", "bytes_up", "bytes_down", "fqdn"]]

    # Compress to days
    flows = flows.reset_index()
    flows["start_bin"] = flows["start"].dt.floor("d")
    flows = flows.set_index("start_bin")

    # Groupby will drop None category values, so manually reassign to other
    flows["category"] = flows["category"].fillna("Other")

    # Do the grouping
    flows = flows.groupby(["start_bin", "category", "fqdn"]).sum()
    flows = flows.compute()

    bok.pd_infra.clean_write_parquet(flows, outfile)


def make_plot(infile):
    grouped_flows = bok.pd_infra.read_parquet(infile)
    grouped_flows = grouped_flows.reset_index()
    grouped_flows["bytes_total"] = grouped_flows["bytes_up"] + grouped_flows["bytes_down"]

    # test = grouped_flows
    # test = grouped_flows.loc[grouped_flows["category"] == None].groupby("fqdn").sum()

    # print(test.sort_values("bytes_total"))

    # Consolidate by week instead of by day
    grouped_flows = grouped_flows[["start_bin", "bytes_total", "category"]].groupby([pd.Grouper(key="start_bin", freq="W-MON"), "category"]).sum()

    grouped_flows = grouped_flows.reset_index()

    print(grouped_flows)
    print(grouped_flows["category"].unique())

    grouped_flows["GB"] = grouped_flows["bytes_total"] / (1000**3)
    alt.Chart(grouped_flows).mark_area().encode(
        x=alt.X("start_bin:T",
                title="Time",
                axis=alt.Axis(labels=True),
                ),
        y=alt.Y("sum(GB):Q",
                title="Fraction of Traffic Per Week(GB)",
                stack="normalize",
                ),
        # shape="direction",
        color="category",
        detail="category",
    ).properties(
        # title="Local Service Use",
        width=500,
    ).interactive().show()

    # .save("renders/bytes_per_category.png",
    #    scale_factor=2
    #    )

    # alt.Chart(grouped_flows).mark_area().encode(
    #     x=alt.X("start_bin:T",
    #             title="Time",
    #             axis=alt.Axis(labels=True),
    #             ),
    #     y=alt.Y("sum(GB):Q",
    #             title="Total Traffic Per Week(GB)",
    #             ),
    #     # shape="direction",
    #     color="name",
    #     detail="name",
    # ).properties(
    #     # title="Local Service Use",
    #     width=500,
    # ).save(
    #        scale_factor=2
    #        )


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    graph_temporary_file = "scratch/graphs/bytes_per_category"
    reduce_to_pandas(outfile=graph_temporary_file, dask_client=client)
    chart = make_plot(graph_temporary_file)
