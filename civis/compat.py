from __future__ import print_function

import six

if six.PY3:
    from unittest import mock
    from functools import lru_cache
    from inspect import signature
    FileNotFoundError = FileNotFoundError  # noqa
else:
    try:
        import mock  # noqa
    except ImportError:  # dev dependency
        pass
    from functools32 import lru_cache  # noqa
    from funcsigs import signature  # noqa
    FileNotFoundError = OSError

if six.PY3:
    from tempfile import TemporaryDirectory
else:
    # Backport TemporaryDirectory; this was introduced in Python 3.2
    from tempfile import mkdtemp
    import shutil as _shutil
    import sys as _sys
    import warnings as _warnings

    class ResourceWarning(Warning):
        pass

    class TemporaryDirectory(object):
        """Create and return a temporary directory.  This has the same
        behavior as mkdtemp but can be used as a context manager.  For
        example:

            with TemporaryDirectory() as tmpdir:
                ...

        Upon exiting the context, the directory and everything contained
        in it are removed.

        This is a port of the Python 3.2+ TemporaryDirectory object,
        modified slightly to work with Python 2.7. Python 3 docs are at
        https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory
        """

        def __init__(self, suffix='', prefix='tmp', dir=None):
            self._closed = False
            self.name = None  # Handle mkdtemp raising an exception
            self.name = mkdtemp(suffix, prefix, dir)

        def __repr__(self):
            return "<{} {!r}>".format(self.__class__.__name__, self.name)

        def __enter__(self):
            return self.name

        def __exit__(self, exc, value, tb):
            self.cleanup()

        def __del__(self):
            # Issue a ResourceWarning if implicit cleanup needed
            self.cleanup(_warn=True)

        def cleanup(self, _warn=False):
            if self.name and not self._closed:
                try:
                    _shutil.rmtree(self.name)
                except (TypeError, AttributeError) as ex:
                    # Issue #10188: Emit a warning on stderr
                    # if the directory could not be cleaned
                    # up due to missing globals
                    if "None" not in str(ex):
                        raise
                    print("ERROR: {!r} while cleaning up "
                          "{!r}".format(ex, self,),
                          file=_sys.stderr)
                    return
                self._closed = True
                if _warn:
                    _warnings.warn("Implicitly cleaning up {!r}".format(self),
                                   ResourceWarning)
