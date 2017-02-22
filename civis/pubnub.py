import warnings

from civis.futures import CivisFuture


class SubscribableResult(CivisFuture):
    def __init__(self, *args, **kwargs):
        warnings.warn('This class was deprecated in version 1.3 in favor of '
                      'civis.futures.CivisFuture. This will be removed in '
                      'version 2.0.',
                      DeprecationWarning)
        super().__init__(*args, **kwargs)
