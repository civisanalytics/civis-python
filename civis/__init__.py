import os

from civis._version import __version__
from civis.civis import APIClient, find, find_one
from civis.service_client import ServiceClient
from civis import io, ml, parallel, utils


CIVIS_JOB_ID = int(os.getenv("CIVIS_JOB_ID", "0"))
CIVIS_RUN_ID = int(os.getenv("CIVIS_RUN_ID", "0"))

__all__ = ["__version__", "APIClient", "find", "find_one", "io",
           "ml", "parallel", "ServiceClient", "utils",
           "CIVIS_JOB_ID", "CIVIS_RUN_ID"]
