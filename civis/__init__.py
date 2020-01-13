from __future__ import absolute_import

import six
from sys import version_info
import warnings

from civis._version import __version__
from civis.civis import APIClient, find, find_one
from civis.service_client import ServiceClient
from civis import io, ml, parallel, utils

if six.PY2:
    warnings.warn("Support for Python 2 is deprecated and will be "
                  "removed in the next version release after "
                  "April 1, 2020.", FutureWarning)
if version_info.major == 3 and version_info.minor == 4:
    warnings.warn("Support for Python 3.4 is deprecated and will be "
                  "removed in the next version release after "
                  "April 1, 2020.", FutureWarning)


__all__ = ["__version__", "APIClient", "find", "find_one", "io",
           "ml", "parallel", "ServiceClient", "utils"]
