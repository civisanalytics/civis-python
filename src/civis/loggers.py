import logging
import os
import sys


class _LogFilter(logging.Filter):
    def __init__(self, mode, level):
        super().__init__()
        if mode not in ("at_or_below", "above"):
            raise ValueError(f"mode must be one of {{at_or_below, above}}): {mode}")
        self.mode = mode
        self.level = level

    def filter(self, record):
        if self.mode == "at_or_below":
            return record.levelno <= self.level
        else:
            return record.levelno > self.level


def civis_logger(name=None, level=None, fmt="%(message)s"):
    """Return a logger for Civis Platform jobs.

    The logs of Civis Platform jobs format stdout in black and stderr in red.
    This logger sends INFO-level (or below) logging to stdout (black),
    and other levels' logging (WARNING, etc.) to stderr (red).

    Parameters
    ----------
    name : str, optional
        Logger name, to be passed into :func:`logging.getLogger`.
        If ``None`` or not provided, ``__name__`` of the module where
        this logger is instantiated is used.
    level : int or str, optional
        Level from which logging is done,
        see https://docs.python.org/3/library/logging.html#logging-levels.
        If ``None`` or not provided, the level specified by the environment
        variable ``CIVIS_LOG_LEVEL`` is used
        (e.g., ``export CIVIS_LOG_LEVEL=DEBUG``).
        If this environment variable is also not given,
        the logging level defaults to ``logging.INFO``.
    fmt : str or logging.Formatter, optional
        Logging format. The default is ``"%(message)s"``.
        For the attributes that can be formatted, see:
        https://docs.python.org/3/library/logging.html#logrecord-objects
        Alternatively, you may pass in a :class:`logging.Formatter` instance
        for more custom formatting.

    Returns
    -------
    :class:`logging.Logger`
    """
    logger = logging.getLogger(name if name is not None else globals()["__name__"])

    if level is None:
        logger.setLevel(os.getenv("CIVIS_LOG_LEVEL") or logging.INFO)
    else:
        logger.setLevel(level)

    # When running on Civis Platform (as opposed to unit tests in CI),
    # we don't want to propagate log records to the root logger
    # in order to avoid duplicate logs.
    logger.propagate = False

    if isinstance(fmt, logging.Formatter):
        platform_fmt = fmt
    else:
        platform_fmt = logging.Formatter(fmt)

    at_or_below_info_hdlr = logging.StreamHandler(sys.stdout)
    at_or_below_info_hdlr.addFilter(_LogFilter("at_or_below", logging.INFO))
    at_or_below_info_hdlr.setFormatter(platform_fmt)
    logger.addHandler(at_or_below_info_hdlr)

    above_info_hdlr = logging.StreamHandler(sys.stderr)
    above_info_hdlr.addFilter(_LogFilter("above", logging.INFO))
    above_info_hdlr.setFormatter(platform_fmt)
    logger.addHandler(above_info_hdlr)

    return logger
