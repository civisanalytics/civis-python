import importlib
import sys
from importlib.metadata import version

from civis.client import APIClient
from civis.loggers import civis_logger
from civis.response import find, find_one
from civis.service_client import ServiceClient


def _lazy_import(name):
    # https://docs.python.org/3/library/importlib.html#implementing-lazy-imports
    spec = importlib.util.find_spec(name)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


io = _lazy_import("civis.io")
ml = _lazy_import("civis.ml")
parallel = _lazy_import("civis.parallel")
utils = _lazy_import("civis.utils")

__version__ = version("civis")
__all__ = [
    "__version__",
    "APIClient",
    "find",
    "find_one",
    "io",
    "civis_logger",
    "ml",
    "parallel",
    "ServiceClient",
    "utils",
]
