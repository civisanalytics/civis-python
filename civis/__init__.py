from __future__ import absolute_import

import six
import warnings

from civis._version import __version__
from civis.civis import APIClient, find, find_one
from civis import io, ml, parallel, utils

if six.PY2:
    warnings.warn("Support for Python 2 is deprecated will be "
                  "removed in the next version release after "
                  "April 1, 2020.", DeprecationWarning)


__all__ = ["__version__", "APIClient", "find", "find_one", "io",
           "ml", "parallel", "utils"]
