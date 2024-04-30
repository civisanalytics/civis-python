import logging
import sys

from civis import CIVIS_JOB_ID, CIVIS_RUN_ID


class _LogFilter(logging.Filter):
    def __init__(self, mode, level):
        super().__init__()
        if mode not in ("at_or_below", "above"):
            raise ValueError(
                f"mode must be one of {{at_or_below, above}}): {mode}"
            )
        self.mode = mode
        self.level = level

    def filter(self, record):
        if self.mode == "at_or_below":
            return record.levelno <= self.level
        else:
            return record.levelno > self.level


def civis_logger(name=None, level=logging.INFO, fmt="%(message)s"):
    """Get a logger for Civis Platform jobs.

    The logs of Civis Platform jobs format stdout in black and stderr in red.
    This logger logs at the INFO level or below to be in black,
    and other levels' logging (WARNING, etc.) in red.

    Parameters
    ----------
    name : str, optional
        Logger name, to be passed into ``logging.getLogger``.
        If ``None`` or not provided, ``__name__`` of the module where
        this logger is instantiated is used.
    level : int or str, optional
        Level from which logging is done,
        e.g., ``logging.INFO`` (default), ``"INFO"``, etc.
        See https://docs.python.org/3/library/logging.html#logging-levels.
    fmt : str or logging.Formatter, optional
        Logging format. The default is ``"%(message)s"``.
        For the attributes that can be formatted, see:
        https://docs.python.org/3/library/logging.html#logrecord-objects
        Alternatively, you may pass in a ``logging.Formatter`` instance
        for more custom formatting.

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(
        name if name is not None else globals()["__name__"]
    )
    logger.setLevel(level)

    # When running on Civis Platform (as opposed to unit tests in CI),
    # we don't want to propagate log records to the root logger
    # in order to avoid duplicate logs.
    # But in running unit tests we do want to leave `propagate` as `True`,
    # or else all the logging/caplog tests would fail.
    # The user can set the `propagate` attribute of the resulting logger
    # back to True if they so choose.
    if CIVIS_JOB_ID and CIVIS_RUN_ID:
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
