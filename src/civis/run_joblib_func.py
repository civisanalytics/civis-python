"""
This is an executable intended for use with a joblib backend
for the Civis platform. It takes a Civis File ID representing
a callable serialized by either ``pickle`` or ``cloudpickle``
as an argument, downloads the file,
deserializes it, calls the callable, serializes the result,
and uploads the result to another Civis File. The output file's ID
will be set as an output on this run.
"""

from datetime import datetime, timedelta
from io import BytesIO
import os
import pickle  # nosec
import sys

import civis
import cloudpickle
from joblib import parallel_config

from civis.parallel import (
    _robust_pickle_download,
    _robust_file_to_civis,
    _setup_remote_backend,
)


def worker_func(func_file_id):
    # Have the output File expire in 7 days.
    expires_at = (datetime.now() + timedelta(days=7)).isoformat()

    client = civis.APIClient()
    job_id = os.environ.get("CIVIS_JOB_ID")
    run_id = os.environ.get("CIVIS_RUN_ID")
    if not job_id or not run_id:
        raise RuntimeError("This function must be run inside a " "Civis container job.")

    # Run the function.
    result = None
    try:
        func, remote_backend = _robust_pickle_download(
            func_file_id, client=client, n_retries=5, delay=0.5
        )

        _backend = _setup_remote_backend(remote_backend)

        with parallel_config(_backend):
            result = func()
    except Exception as exc:
        print("Error! Attempting to record exception.")
        result = exc
        raise
    finally:
        # Serialize the result and upload it to the Files API.
        if result is not None:
            # If the function exits without erroring, we may not have a result.
            result_buffer = BytesIO()
            cloudpickle.dump(result, result_buffer, pickle.HIGHEST_PROTOCOL)
            result_buffer.seek(0)
            output_name = "Results from Joblib job {} / run {}".format(job_id, run_id)
            output_file_id = _robust_file_to_civis(
                result_buffer,
                output_name,
                n_retries=5,
                delay=0.5,
                expires_at=expires_at,
                client=client,
            )
            client.scripts.post_containers_runs_outputs(
                job_id, run_id, "File", output_file_id
            )
            print("Results output to file ID: {}".format(output_file_id))


def main():
    if len(sys.argv) > 1:
        func_file_id = sys.argv[1]
    else:
        # If the file ID to download isn't given as a command-line
        # argument, assume that it's in an environment variable.
        func_file_id = os.environ["JOBLIB_FUNC_FILE_ID"]
    worker_func(func_file_id=func_file_id)


if __name__ == "__main__":
    main()
