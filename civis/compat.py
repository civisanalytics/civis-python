# flake8: noqa

import six

if six.PY3:
    from unittest import mock
    from functools import lru_cache
    from inspect import signature
    FileNotFoundError = FileNotFoundError
else:
    try:
        import mock
    except ImportError:  # dev dependency
        pass
    from functools32 import lru_cache
    from funcsigs import signature
    FileNotFoundError = IOError
