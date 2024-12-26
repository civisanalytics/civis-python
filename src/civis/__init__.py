import importlib
import sys
from importlib.metadata import version
from typing import TYPE_CHECKING

from civis.client import APIClient
from civis.loggers import civis_logger
from civis.response import find, find_one, Response, PaginatedResponse
from civis.service_client import ServiceClient


try:
    # __sphinx_build__ is injected into bulitins in docs/conf.py.
    if __sphinx_build__:
        _BUILDING_SPHINX_DOC = True
except NameError:
    _BUILDING_SPHINX_DOC = False


def _lazy_import(name):
    # https://docs.python.org/3/library/importlib.html#implementing-lazy-imports
    spec = importlib.util.find_spec(name)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


if TYPE_CHECKING or _BUILDING_SPHINX_DOC:
    # Regularly loaded modules are needed for typing information and doc generation.
    from civis import futures, io, ml, parallel, utils, workflows
else:
    futures = _lazy_import("civis.futures")
    io = _lazy_import("civis.io")
    ml = _lazy_import("civis.ml")
    parallel = _lazy_import("civis.parallel")
    utils = _lazy_import("civis.utils")
    workflows = _lazy_import("civis.workflows")

__version__ = version("civis")
__all__ = [
    "__version__",
    "APIClient",
    "find",
    "find_one",
    "futures",
    "io",
    "civis_logger",
    "ml",
    "PaginatedResponse",
    "parallel",
    "Response",
    "ServiceClient",
    "utils",
    "workflows",
]
