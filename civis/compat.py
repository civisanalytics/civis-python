import six

if six.PY3:
    from unittest import mock
    from functools import lru_cache
else:
    import mock
    from functools32 import lru_cache
    from funcsigs import signature
