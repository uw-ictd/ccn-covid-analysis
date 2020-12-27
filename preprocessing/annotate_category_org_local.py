import datetime
import pandas as pd
import dask.dataframe
import dask.distributed
import dask.delayed
import ipaddress
import os

from mappers.mapped_ips import IpProcessor
import infra.dask
import mappers.domains
import infra.platform

from collections import namedtuple
_StunFlow = namedtuple("StunFlow", ["expiration", "flow", "is_setup"])


def _categorize_user(in_path, out_path):
    """Run categorization over the input parquet file and write to the output

    Requires the input parquet file specify an `fqdn` column, protocol, and ports
    """
    frame = infra.dask.read_parquet(in_path)

    # First pass assign by FQDN
    processor = mappers.domains.FqdnProcessor()
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

    # Assign remaining unknown by ip
    frame["org"] = frame.apply(
        lambda row: _assign_org_from_ip(row["dest_ip"], row["org"]),
        axis="columns",
        meta=("org", object)
    )

    # Assign remaining unknown by ip
    frame["category"] = frame.apply(
        lambda row: _assign_category_from_ip(row["dest_ip"], row["category"]),
        axis="columns",
        meta=("category", object)
    )

    # Lastly assign local by IP type
    frame["local"] = frame.apply(
        lambda row: _annotate_local(row["dest_ip"]),
        axis="columns",
        meta=("local", bool))

    return infra.dask.clean_write_parquet(frame, out_path, compute=False)


def _assign_org_from_ip(ip, current):
    if current != "Unknown (No DNS)":
        return current

    if ("157.240.25." in ip) or ("157.240.24." in ip) or ("157.240.2." in ip):
        return "Facebook"
    else:
        return current


def _assign_category_from_ip(ip, current):
    if current != "Unknown (No DNS)":
        return current

    if ("157.240.25." in ip) or ("157.240.24." in ip) or ("157.240.2." in ip):
        return "Unknown (Not Mapped)"
    else:
        return current


def _process_cohort_into_out_chunk(cohort, stun_state, out_chunk):
    flow_continue_threshold = datetime.timedelta(seconds=5)

    # The next stun state takes effect after this cohort of flows with tied timestamps is processed.
    next_stun_state = stun_state.copy()
    for cohort_flow in cohort:
        augmented_flow = cohort_flow._asdict()
        if not (cohort_flow.protocol == 17 and cohort_flow.dest_port == 3478):
            # See if this flow is actually a stun flow
            for stun_record in stun_state:
                stun_flow = stun_record.flow
                if ((stun_record.is_setup and (cohort_flow.user_port == stun_flow.user_port)) or
                    ((cohort_flow.user_port == stun_flow.user_port) and
                     (cohort_flow.dest_port == stun_flow.dest_port) and
                     (cohort_flow.protocol == stun_flow.protocol) and
                     (cohort_flow.dest_ip == stun_flow.dest_ip))):
                    if ((((cohort_flow.category == "Unknown (No DNS)") or
                          (cohort_flow.category == "Unknown (Not Mapped)")) and
                         ("Unknown" in cohort_flow.org)) or
                        ("emome-ip.hinet.net" in cohort_flow.fqdn)):
                        # These appear to be generic dns records for users within Chunghwa Telecom (in Taiwan)
                        augmented_flow["category"] = "Peer to Peer"
                        augmented_flow["org"] = "ICE Peer (Unknown Org)"
                        # Don't want to add stun flows back accidentally
                        if stun_flow != cohort_flow:
                            expiration_time = cohort_flow.end + flow_continue_threshold
                            next_stun_state.add(
                                _StunFlow(expiration_time, cohort_flow, is_setup=False)
                            )
                    elif ("turnservice" in cohort_flow.fqdn or
                          "facebook" in cohort_flow.fqdn or
                          "Facebook" == cohort_flow.org or
                          cohort_flow.category == "Unknown (Not Mapped)" or
                          cohort_flow.category == "API"):
                        # Update the category for stun flows from known orgs otherwise unclassified or generic.
                        augmented_flow["category"] = "ICE (STUN/TURN)"
                        # Don't want to add stun flows back accidentally
                        if stun_flow != cohort_flow:
                            expiration_time = cohort_flow.end + flow_continue_threshold
                            next_stun_state.add(
                                _StunFlow(expiration_time, cohort_flow, is_setup=False)
                            )
                    else:
                        # There is a bit of noise from port reuse.
                        print("Not overriding existing category", cohort_flow.category)
                        print(cohort_flow.fqdn)

        out_chunk.append(augmented_flow)
    return out_chunk, next_stun_state


def _augment_user_flows_with_stun_state(in_path, out_path):
    """Iterate over the flows and track STUN/TURN state for the user

    Will not be correct unless the flow is indexed by time
    """
    flow_frame = infra.dask.read_parquet(in_path)
    # Bookeeping for building a new frame
    max_rows_per_division = 100000
    out_chunk = list()
    out_frame = None

    stun_state = set()
    # Consider stun activity stale after 1 minutes. This could technically
    # miss repeated calls. There is a tradeoff though since increasing the
    # time increases the chance of incidental reuse of the port on the client.
    expiry_threshold = datetime.timedelta(minutes=1)
    current_timestamp = None
    current_timestamp_cohort = set()
    for i, flow in enumerate(flow_frame.itertuples()):
        flow_start_time = flow.Index

        if current_timestamp is None:
            current_timestamp = flow_start_time

        # Build a list of all flows tied at the current time, since the
        # timestamps are not very precise and the ordering is not stable.
        if flow_start_time == current_timestamp:
            current_timestamp_cohort.add(flow)
        else:
            # Do all processing of the built timestamp cohort first,
            # then address the flow from this iteration.

            # Expire old stun requests
            valid_stun_state = set()
            for stun_flow in stun_state:
                if stun_flow.expiration > current_timestamp:
                    valid_stun_state.add(stun_flow)
            stun_state = valid_stun_state

            # Find any stun flows at the timestamp first before any other processing
            for cohort_flow in current_timestamp_cohort:
                if cohort_flow.protocol == 17 and cohort_flow.dest_port == 3478:
                    # This flow is ICE (STUN/TURN)!
                    # Define the expiration of ICE setup flows from their start time
                    exiration_time = cohort_flow.Index + expiry_threshold
                    stun_state.add(_StunFlow(exiration_time, cohort_flow, is_setup=True))

            # Augment all other flows in the cohort
            out_chunk, stun_state = _process_cohort_into_out_chunk(
                current_timestamp_cohort, stun_state, out_chunk
            )

            if len(out_chunk) >= max_rows_per_division:
                new_frame = dask.dataframe.from_pandas(
                    pd.DataFrame(out_chunk),
                    chunksize=max_rows_per_division,
                    sort=False
                )
                if out_frame is None:
                    out_frame = new_frame
                else:
                    out_frame = out_frame.append(new_frame)
                out_chunk = list()

            # Reset the cohort with a new lead flow from the current iteration
            current_timestamp = flow_start_time
            current_timestamp_cohort = {flow}

    # Handle the dangling cohort and chunk on loop termination
    out_chunk, stun_state = _process_cohort_into_out_chunk(
        current_timestamp_cohort, stun_state, out_chunk
    )

    if len(out_chunk) > 0:
        new_frame = dask.dataframe.from_pandas(
            pd.DataFrame(out_chunk),
            chunksize=max_rows_per_division)
        if out_frame is None:
            out_frame = new_frame
        else:
            out_frame = out_frame.append(new_frame)

    if out_frame is None:
        print("User has no flows in the current dataset slice, returning early")
        print(in_path)
        return

    out_frame = out_frame.rename(columns={"Index": "start"})
    out_frame = out_frame.set_index("start").repartition(partition_size="128M",
                                                         force=True)
    out_frame = out_frame.categorize(columns=["fqdn_source", "org", "category"])

    infra.dask.clean_write_parquet(out_frame, out_path)
    print("Finished writing user", in_path)


def augment_all_user_flows(in_parent_directory, out_parent_directory, client):
    users_in_flow_log = sorted(os.listdir(in_parent_directory))
    tokens = []
    for user in users_in_flow_log:
        print("Doing category augmentation for user:", user)
        in_user_directory = os.path.join(in_parent_directory, user)
        out_user_directory = os.path.join(out_parent_directory, user)

        compute_token = _categorize_user(in_user_directory, out_user_directory)
        tokens.append(compute_token)

    print("Starting dask category augmentation computation")
    client.compute(tokens, sync=True)
    print("Completed category augmentation")


def stun_augment_all_user_flows(in_parent_directory, out_parent_directory, client):
    users_in_flow_log = sorted(os.listdir(in_parent_directory))
    tokens = []
    max_parallel_users = 60
    for i, user in enumerate(users_in_flow_log):
        print("Doing STUN state tracking for user:", user)
        in_user_directory = os.path.join(in_parent_directory, user)
        out_user_directory = os.path.join(out_parent_directory, user)

        compute_token = dask.delayed(_augment_user_flows_with_stun_state)(in_user_directory, out_user_directory)
        tokens.append(compute_token)

        if (i % max_parallel_users) == (max_parallel_users - 1):
            print("Starting dask stun intermediate computation")
            client.compute(tokens, sync=True)
            tokens = []

    print("Starting dask stun final computation")
    if len(tokens) > 0:
        client.compute(tokens, sync=True)
    print("Completed STUN augmentation")


def _annotate_local(address):
    return not ipaddress.ip_address(address).is_global


def merge_parquet_frames(in_parent_directory, out_frame_path):
    """Iterate through divs in a parent directory and merge to the out frame
    """
    merged_frame = None
    div_on_disk = sorted(os.listdir(in_parent_directory))
    for div in div_on_disk:
        div_path = os.path.join(in_parent_directory, div)
        frame = infra.dask.read_parquet(div_path)

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

    infra.dask.clean_write_parquet(merged_frame, out_frame_path)


def _print_heavy_hitter_unmapped_domains(infile):
    df = infra.dask.read_parquet(infile)

    unmapped = df.loc[((df["org"] == "Unknown (Not Mapped)") | (df["category"] == "Unknown (Not Mapped)"))]
    df = unmapped.groupby("fqdn").sum()

    panda = df.compute()
    print("Downlinks:")
    print("----------")
    print(panda.sort_values("bytes_down", ascending=False).head(50))

    print("Uplinks:")
    print("--------")
    print(panda.sort_values("bytes_up", ascending=False).head(50))


def annotate_category_org_from_ip(in_path, out_path):
    flows = infra.dask.read_parquet(in_path)
    flows = flows.reset_index()
    flows = flows.astype({
        "org": object,
        "category": object,
    })

    ip_processor = IpProcessor()
    flows["iporg"] = flows.loc[
        (flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")
        ].apply(
        lambda row: ip_processor.process_ip(row["dest_ip"])[0],
        axis=1,
        meta=("iporg", object),
    )

    flows["ipcategory"] = flows.loc[
        (flows["category"] == "Unknown (No DNS)") | (flows["org"] == "Unknown (No DNS)")
        ].apply(
        lambda row: ip_processor.process_ip(row["dest_ip"])[1],
        axis=1,
        meta=("ipcategory", object),
    )

    flows["category"] = flows["category"].mask(
        ((flows["category"] == "Unknown (No DNS)") & (flows["ipcategory"] != None)),
        other=flows["ipcategory"]
    )
    flows["org"] = flows["org"].mask(
        ((flows["org"] == "Unknown (No DNS)") & (flows["iporg"] != None)),
        other=flows["iporg"]
    )

    flows = flows.drop(["ipcategory", "iporg"], axis=1)
    # flows = flows.categorize(columns=["fqdn_source", "org", "category"])
    infra.dask.clean_write_parquet(flows, out_path)


def annotate_all(client):
    in_parent_directory = "scratch/flows/typical_fqdn_DIV_user_INDEX_start/"
    annotated_parent_directory = "scratch/flows/typical_fqdn_category_org_local_DIV_user_INDEX_start"
    stun_annotated_parent_directory = "scratch/flows/typical_fqdn_category_stun_org_local_DIV_user_INDEX_start"
    merged_out_directory = "scratch/flows/typical_fqdn_org_category_local_no_ip_DIV_none_INDEX_start"
    final_out_directory = "scratch/flows/typical_fqdn_org_category_local_DIV_none_INDEX_start"

    augment_all_user_flows(in_parent_directory, annotated_parent_directory, client)
    stun_augment_all_user_flows(annotated_parent_directory, stun_annotated_parent_directory, client)
    merge_parquet_frames(stun_annotated_parent_directory, merged_out_directory)
    annotate_category_org_from_ip(merged_out_directory, final_out_directory)


if __name__ == "__main__":
    platform = infra.platform.read_config()
    dask_client = infra.dask.setup_platform_tuned_dask_client(20, platform)

    annotate_all(dask_client)

    dask_client.close()

    # print("Temporary computation to find large domains.")
    # _print_heavy_hitter_unmapped_domains("scratch/flows/unmapped_typical_fqdn_org_category_local_TM_DIV_none_INDEX_start")

    print("Finished heavy compute operations")
    print("Exited")


