""" Compute gaps where there are no flow logs in progress

Take advantage of the fact that flowlogs exist in memory before being saved.
Times where there are no logs in progress at all, across any user,
could indicate power or enodeb failures.
"""

import pandas as pd
import os.path

import infra.platform
import infra.dask
import infra.pd


def _reduce_flow_gaps_to_pandas(typical_flows_file, p2p_flows_file, outfile, dask_client):
    typical_flows = infra.dask.read_parquet(typical_flows_file)[["bytes_up", "bytes_down", "end"]]
    peer_flows = infra.dask.read_parquet(p2p_flows_file)[["bytes_a_to_b", "bytes_b_to_a", "end"]]

    # Compute total bytes and drop intermediates
    typical_flows["total_bytes"] = typical_flows["bytes_up"] + typical_flows["bytes_down"]
    peer_flows["total_bytes"] = peer_flows["bytes_a_to_b"] + peer_flows["bytes_b_to_a"]
    typical_flows = typical_flows.drop(["bytes_up", "bytes_down"], axis="columns")
    peer_flows = peer_flows.drop(["bytes_a_to_b", "bytes_b_to_a"], axis="columns")

    # Combine into one master flow frame
    all_flows = typical_flows.append(
        peer_flows, interleave_partitions=True
    )

    # Sort all flows by time.
    all_flows = all_flows.reset_index().set_index("start")

    # Recover the start time
    all_flows = all_flows.reset_index()

    # Manually iterate since rolling for datetimes is not implemented.
    last_end = None
    sanity_start = None
    gaps = []
    for i, row in enumerate(all_flows.itertuples()):
        if (i % 10000 == 0):
            print("Processing row {}".format(i))

        # Ensure sorted
        if (sanity_start is not None) and (row.start < sanity_start):
            print("INSANE")
            print(i)
            print(row.start)
            print(sanity_start)
            raise RuntimeError("Failed")

        if last_end is not None and row.start > last_end:
            gaps.append((last_end, row.start))

        last_end = row.end
        sanity_start = row.start

    print("Gaps length", len(gaps))
    # Gaps is small so just use pandas
    gaps_frame = pd.DataFrame(gaps, columns=["start", "end"])
    print(gaps_frame.head())

    infra.pd.clean_write_parquet(gaps_frame, outfile)


def run(dask_client, basedir):
    typical_flows_file = os.path.join(basedir, "data/clean/flows/typical_fqdn_category_local_TM_DIV_none_INDEX_start")
    p2p_flows_file = os.path.join(basedir, "data/clean/flows/p2p_TM_DIV_none_INDEX_start")
    out_file = os.path.join(basedir, "data/derived/log_gaps_TM.parquet")
    if dask_client is not None:
        _reduce_flow_gaps_to_pandas(typical_flows_file, p2p_flows_file, out_file, dask_client)


if __name__ == "__main__":
    platform = infra.platform.read_config()
    basedir = "../"

    if platform.large_compute_support:
        client = infra.dask.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
    else:
        client = None

    run(client, basedir)

    if client is not None:
        client.close()

    print("Done!")
