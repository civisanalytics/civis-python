import warnings

from civis.futures import CivisFuture


def SubscribableResult(*args, **kwargs):
    warnings.warn('This class was deprecated in version 1.3 in favor of '
                  'civis.futures.CivisFuture. This will be removed in 1.4.',
                  DeprecationWarning)
    return CivisFuture(*args, **kwargs)
