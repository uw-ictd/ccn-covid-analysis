import datetime
import pandas as pd
import dask.dataframe
import dask.distributed
import ipaddress
import os

import bok.dask_infra
import bok.domains


def _categorize_user(in_path, out_path):
    """Run categorization over the input parquet file and write to the output

    Requires the input parquet file specify an `fqdn` column, protocol, and ports
    """
    frame = bok.dask_infra.read_parquet(in_path)

    # First pass assign by FQDN
    processor = bok.domains.FqdnProcessor()
    frame["org_category"] = frame.apply(
        lambda row: processor.process_fqdn(row["fqdn"]),
        axis="columns",
        meta=("org_category", object))

    frame["org"] = frame.apply(
        lambda row: row["org_category"][0],
        axis="columns",
        meta=("org", object))

    frame["category"] = frame.apply(
        lambda row: row["org_category"][1],
        axis="columns",
        meta=("category", object))
    frame = frame.drop("org_category", axis=1)

    # Second pass assign by specialized protocols
    frame["category"] = frame["category"].mask(
        ((frame["protocol"] == 17) & (frame["dest_port"] == 3478)),
        "ICE (STUN/TURN)",
    )

    # Third pass to track stun state and otherwise unknown peer IPs
    # TODO(matt9j) Check the column types are preserved
    print("Frame before annotation")
    print(frame)
    frame = _augment_user_flows_with_stun_state(frame)
    print("Frame after annotation")
    print(frame)

    # Lastly assign local by IP type
    frame["local"] = frame.apply(
        lambda row: _annotate_local(row["dest_ip"]),
        axis="columns",
        meta=("local", bool))

    return bok.dask_infra.clean_write_parquet(frame, out_path, compute=False)


def _augment_user_flows_with_stun_state(flow_frame):
    """Iterate over the flows and track STUN/TURN state for the user

    Will not be correct unless the flow is indexed by time
    """

    # Bookeeping for building a new frame
    max_rows_per_division = 10000
    out_chunk = list()
    out_frame = None

    stun_state = set()
    # Consider stun activity stale after 5 minutes. This could technically
    # miss repeated calls. There is a tradeoff though since increasing the
    # time increases the chance of incidental reuse of the port on the client.
    expiry_threshold = datetime.timedelta(minutes=5)
    for i, flow in enumerate(flow_frame.itertuples()):
        if i % 10000 == 0:
            print("Processed flow", i)

        flow_start_time = flow.Index
        augmented_flow = flow._asdict()

        # Expire old stun requests
        valid_stun_state = set()
        for stun_flow in stun_state:
            if (flow_start_time - stun_flow.Index) < expiry_threshold:
                valid_stun_state.add(stun_flow)
        stun_state = valid_stun_state

        if flow.protocol == 17 and flow.dest_port == 3478:
            # This flow is ICE (STUN/TURN)!
            stun_state.add(flow)
        else:
            # See if this flow is actually a stun flow
            for stun_flow in stun_state:
                if flow.user_port == stun_flow.user_port:
                    if ((flow.category != "Unknown (No DNS)") and
                            (flow.category != "Unknown (Not Mapped)")):
                        print("Overwriting category", flow.category)
                    augmented_flow["category"] = "Peer to Peer"

        out_chunk.append(augmented_flow)
        if len(out_chunk) >= max_rows_per_division:
            new_frame = dask.dataframe.from_pandas(
                pd.DataFrame(out_chunk),
                chunksize=max_rows_per_division,
            )
            if out_frame is None:
                out_frame = new_frame
            else:
                out_frame = out_frame.append(new_frame)
            out_chunk = list()

    if len(out_chunk) > 0:
        new_frame = dask.dataframe.from_pandas(
            pd.DataFrame(out_chunk),
            chunksize=max_rows_per_division)
        if out_frame is None:
            out_frame = new_frame
        else:
            out_frame = out_frame.append(new_frame)

    out_frame = out_frame.rename(columns={"Index": "start"})
    out_frame = out_frame.set_index("start", sorted=True).repartition(partition_size="64M",
                                                                      force=True)
    out_frame = out_frame.categorize(columns=["fqdn_source", "category"])
    return out_frame


def augment_all_user_flows(in_parent_directory, out_parent_directory, client):
    users_in_flow_log = sorted(os.listdir(in_parent_directory))
    tokens = []
    for user in users_in_flow_log:
        print("Doing STUN state tracking for user:", user)
        in_user_directory = os.path.join(in_parent_directory, user)
        out_user_directory = os.path.join(out_parent_directory, user)

        compute_token = _categorize_user(in_user_directory, out_user_directory)
        tokens.append(compute_token)

    print("Starting computation")
    client.compute(tokens, sync=True)

    print("Completed category augmentation")


def _annotate_local(address):
    return not ipaddress.ip_address(address).is_global


def merge_parquet_frames(in_parent_directory, out_frame_path):
    """Iterate through divs in a parent directory and merge to the out frame
    """
    merged_frame = None
    div_on_disk = sorted(os.listdir(in_parent_directory))
    for div in div_on_disk:
        div_path = os.path.join(in_parent_directory, div)
        frame = bok.dask_infra.read_parquet(div_path)

        if merged_frame is None:
            merged_frame = frame
        else:
            merged_frame = merged_frame.append(frame)

    merged_frame = merged_frame.reset_index().set_index(
        "start"
    ).repartition(
        partition_size="64M",
        force=True
    )

    bok.dask_infra.clean_write_parquet(merged_frame, out_frame_path)


if __name__ == "__main__":
    client = bok.dask_infra.setup_dask_client()
    print("To see execution status, check out the dask status page at localhost:8787 while the computation is running.")

    in_parent_directory = "scratch/flows/typical_fqdn_TM_DIV_user_INDEX_start/"
    annotated_parent_directory = "scratch/flows/typical_fqdn_category_org_local_TM_DIV_user_INDEX_start"
    merged_out_directory = "scratch/flows/typical_fqdn_category_org_local_TM_DIV_none_INDEX_start"

    augment_all_user_flows(in_parent_directory, annotated_parent_directory, client)
    merge_parquet_frames(annotated_parent_directory, merged_out_directory)

    client.close()
    print("Exited")


