from __future__ import absolute_import

from civis.futures import (  # noqa
    _ResultPollingThread,
    CivisFuture,
    _DEFAULT_POLLING_INTERVAL)

# keep this for backwards compatibility
PollableResult = CivisFuture
