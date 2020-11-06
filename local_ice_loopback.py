import altair as alt
import numpy as np
import pandas as pd

import infra.dask_infra
import infra.pd_infra
import infra.platform


def compute_cdf(frame, value_column, base_column):
    # Find the PDF first
    stats_frame = frame.groupby(value_column).count()[[base_column]].rename(columns = {base_column: "base_count"})
    stats_frame["pdf"] = stats_frame["base_count"] / sum(stats_frame["base_count"])
    stats_frame["cdf"] = stats_frame["pdf"].cumsum()
    stats_frame = stats_frame.reset_index()
    return stats_frame


def reduce_to_pandas(outpath, client):
    flows = infra.dask_infra.read_parquet(
        "data/clean/flows/typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")[["user", "category", "fqdn", "bytes_up", "bytes_down", "dest_ip"]]

    flows = flows.loc[(flows["category"] == "Peer to Peer") | (flows["category"] == "ICE (STUN/TURN)")]

    flows = flows.groupby(["dest_ip", "fqdn", "user"]).aggregate({"bytes_up": np.sum,
                                                                  "bytes_down": np.sum,
                                                                  })

    flows["total_bytes"] = flows["bytes_up"] + flows["bytes_down"]

    # Do a second groupby to count distinct user entries for each dest_ip/fqdn tuple
    flows = flows.reset_index()
    flows = flows.groupby(["dest_ip", "fqdn"]).aggregate({"bytes_up": np.sum,
                                                                  "bytes_down": np.sum,
                                                                  "total_bytes": np.sum,
                                                                  "user": "count"})

    flows = flows.rename(columns={"user": "user_count"})

    infra.pd_infra.clean_write_parquet(flows.compute(), outpath)


def make_plot(inpath):
    flows = infra.pd_infra.read_parquet(inpath)

    flows = flows.reset_index()
    flows = flows.sort_values("total_bytes")
    flows["total_MB"] = flows["total_bytes"] / (1000**2)

    print(flows[flows["dest_ip"] == "10.10.20.1"])
    print(flows[flows["dest_ip"] == "202.5.163.37"])
    print(flows[flows["dest_ip"] == "202.5.163.38"])
    print(flows[flows["dest_ip"] == "210.89.70.250"])

    flows = flows.sort_values("user_count")
    print(flows)
    flows = flows.loc[(flows["dest_ip"] == "202.5.163.38") | (flows["dest_ip"] == "210.89.70.250")]

    alt.Chart(flows).mark_line(interpolate='step-after', clip=True).encode(
        x=alt.X('day_bin:T',
                #scale=alt.Scale(type="linear", domain=(0, 1.00)),
                #title="Online ratio"
                ),
        y=alt.Y('total',
                #title="Fraction of Users (CDF)",
                #scale=alt.Scale(type="linear", domain=(0, 1.0)),
                ),
        # color=alt.Color(
        #     "ip_a",
        #     legend=alt.Legend(
        #         title=None
        #     ),
        # ),
        # strokeDash="type",
    ).save("renders/ice_rates.png", scale_factor=2.0)


if __name__ == "__main__":
    platform = infra.platform.read_config()

    # Module specific format options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 40)

    graph_temp_file = "scratch/graphs/rate_ice_local_loopback"

    if platform.large_compute_support:
        print("Running compute subcommands")
        client = infra.dask_infra.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
        reduce_to_pandas(graph_temp_file, client)
        client.close()

    if platform.altair_support:
        make_plot(inpath=graph_temp_file)

    print("Done!")
