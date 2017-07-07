"""
This is an executable intended for use with a joblib backend
for the Civis platform. It takes a Civis File ID representing
a joblib-serialized callable as an argument, downloads the file,
deserializes it, calls the callable, serializes the result,
and uploads the result to another Civis File. The output file's ID
will be set as an output on this run.
"""
from __future__ import absolute_import, print_function

from datetime import datetime, timedelta
from io import BytesIO
import os
import sys

import civis
import joblib
from joblib.my_exceptions import TransportableException
from joblib.format_stack import format_exc


def worker_func(func_file_id):
    # Have the output File expire in 7 days.
    expires_at = (datetime.now() + timedelta(days=7)).isoformat()

    client = civis.APIClient()
    job_id = os.environ.get('CIVIS_JOB_ID')
    run_id = os.environ.get('CIVIS_RUN_ID')
    if not job_id or not run_id:
        raise RuntimeError("This function must be run inside a "
                           "Civis container job.")

    func_buffer = BytesIO()
    civis.io.civis_to_file(func_file_id, func_buffer)
    func_buffer.seek(0)
    func = joblib.load(func_buffer)

    # Run the function.
    result = None
    try:
        result = func()
    except Exception:
        print("Error! Attempting to record exception.")
        # Wrap the exception in joblib's TransportableException
        # so that joblib can properly display the results.
        e_type, e_value, e_tb = sys.exc_info()
        text = format_exc(e_type, e_value, e_tb, context=10, tb_offset=1)
        result = TransportableException(text, e_type)
        raise
    finally:
        # Serialize the result and upload it to the Files API.
        # Note that if compress is 0, joblib will output multiple files.
        # compress=3 is a good compromise between space and read/write times
        # (https://github.com/joblib/joblib/blob/18f9b4ce95e8788cc0e9b5106fc22573d768c44b/joblib/numpy_pickle.py#L358).
        if result is not None:
            # If the function exits without erroring, we may not have a result.
            result_buffer = BytesIO()
            joblib.dump(result, result_buffer, compress=3)
            result_buffer.seek(0)
            output_name = "Results from Joblib job {} / run {}".format(job_id,
                                                                       run_id)
            output_file_id = civis.io.file_to_civis(result_buffer, output_name,
                                                    expires_at=expires_at)
            client.scripts.post_containers_runs_outputs(job_id, run_id,
                                                        'File', output_file_id)
            print("Results output to file ID: {}".format(output_name,
                                                         output_file_id))


def main():
    if len(sys.argv) > 1:
        func_file_id = sys.argv[1]
    else:
        # If the file ID to download isn't given as a command-line
        # argument, assume that it's in an environment variable.
        func_file_id = os.environ['JOBLIB_FUNC_FILE_ID']
    worker_func(func_file_id=func_file_id)


if __name__ == '__main__':
    main()
