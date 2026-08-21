"""Machine learning in Civis

.. deprecated:: 2.10.0
    ``civis.ml`` will be removed at civis-python v3.0.0
    (scheduled for release in February 2027).
    The CivisML script templates in Civis Platform are unaffected.
    If you need ``civis.ml``, pin civis-python to v2.x.x.
"""

import warnings

from civis.ml._model import *  # NOQA
from civis.ml._helper import *  # NOQA

_DEPRECATED_MSG = (
    "`civis.ml` is deprecated since civis-python v2.10.0 and will be removed "
    "at civis-python v3.0.0 (scheduled for release in February 2027). "
    "The CivisML script templates in Civis Platform are unaffected. "
    "If you need `civis.ml`, pin civis-python to v2.x.x."
)

warnings.warn(_DEPRECATED_MSG, FutureWarning)
