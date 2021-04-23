from pathlib import Path

import intermediates.compute_log_gaps
import intermediates.compute_user_active_deltas

import infra.dask
import infra.platform


if __name__ == "__main__":
    platform = infra.platform.read_config()
    basedir = "./"

    if platform.large_compute_support:
        client = infra.dask.setup_platform_tuned_dask_client(per_worker_memory_GB=10, platform=platform)
    else:
        print("Without dask compute support some intermediates will not be made!")
        client = None

    # Ensure destination directories exist.
    Path("data/derived").mkdir(parents=True, exist_ok=True)
    Path("scratch/graphs").mkdir(parents=True, exist_ok=True)

    intermediates.compute_log_gaps.run(client, basedir)
    intermediates.compute_user_active_deltas.run(client, basedir)

    if client is not None:
        client.close()
