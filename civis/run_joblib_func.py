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
import warnings

import civis
import cloudpickle
from joblib.my_exceptions import TransportableException
from joblib.format_stack import format_exc
from joblib import parallel_backend as _joblib_para_backend

try:
    with warnings.catch_warnings():
        # Ignore the warning: "DeprecationWarning: sklearn.externals.joblib is
        # deprecated in 0.21 and will be removed in 0.23. Please import this
        # functionality directly from joblib, which can be installed with:
        # pip install joblib. If this warning is raised when loading pickled
        # models, you may need to re-serialize those models with
        # scikit-learn 0.21+."
        warnings.simplefilter('ignore', DeprecationWarning)
        # sklearn 0.22 has switched from DeprecationWarning to FutureWarning
        warnings.simplefilter('ignore', FutureWarning)
        from sklearn.externals.joblib import (
            parallel_backend as _sklearn_para_backend)

        # NO_SKLEARN_BACKEND would be a better name here since it'll be true
        # for future scikit-learn versions that won't include the joblib
        # module as well as when scikit-learn isn't installed, but changing
        # the name would technically be a breaking change.
        NO_SKLEARN = False
except ImportError:
    NO_SKLEARN = True

from civis.parallel import (
    _robust_pickle_download, _robust_file_to_civis, _setup_remote_backend)


def worker_func(func_file_id):
    # Have the output File expire in 7 days.
    expires_at = (datetime.now() + timedelta(days=7)).isoformat()

    client = civis.APIClient()
    job_id = os.environ.get('CIVIS_JOB_ID')
    run_id = os.environ.get('CIVIS_RUN_ID')
    if not job_id or not run_id:
        raise RuntimeError("This function must be run inside a "
                           "Civis container job.")

    # Run the function.
    result = None
    try:
        func, remote_backend = _robust_pickle_download(
            func_file_id, client=client, n_retries=5, delay=0.5)

        _backend = _setup_remote_backend(remote_backend)

        # graceful nested context managers are ~hard across python versions,
        # this just works...
        if NO_SKLEARN:
            with _joblib_para_backend(_backend):
                result = func()
        else:
            # we are using the nested context managers to set the joblib
            # backend to the requested one in both copes of joblib, the
            # package and the copy shipped by sklearn at
            # `sklearn.externals.joblib`. joblib maintains the current
            # backend as global state in the package and thus there are
            # two backends to set when you have two copies of the package
            # in play.
            with _sklearn_para_backend(_backend):
                with _joblib_para_backend(_backend):
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
        if result is not None:
            # If the function exits without erroring, we may not have a result.
            result_buffer = BytesIO()
            cloudpickle.dump(result, result_buffer, pickle.HIGHEST_PROTOCOL)
            result_buffer.seek(0)
            output_name = "Results from Joblib job {} / run {}".format(job_id,
                                                                       run_id)
            output_file_id = _robust_file_to_civis(result_buffer, output_name,
                                                   n_retries=5, delay=0.5,
                                                   expires_at=expires_at,
                                                   client=client)
            client.scripts.post_containers_runs_outputs(job_id, run_id,
                                                        'File', output_file_id)
            print("Results output to file ID: {}".format(output_file_id))


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
