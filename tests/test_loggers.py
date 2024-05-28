import logging
from uuid import uuid4

from civis import civis_logger


def _get_test_logger(*args, **kwargs):
    # Need to use a logger of a different name in each test function,
    # or else we'd hit this issue:
    # https://github.com/pytest-dev/pytest/issues/5577
    logger = civis_logger(name=str(uuid4()), *args, **kwargs)
    # Set `propagate` back to `True`,
    # or else all the logging/caplog tests would fail.
    logger.propagate = True
    return logger


def test_civis_logger_base_case(caplog, capsys):
    log = _get_test_logger()
    caplog.set_level(log.level)

    log.debug("debug level")
    log.info("this is info level")
    log.warning("warning!")
    log.error("error!!")

    actual_logs = [(rec.levelname, rec.message) for rec in caplog.records]
    expected_logs = [
        ("INFO", "this is info level"),
        ("WARNING", "warning!"),
        ("ERROR", "error!!"),
    ]
    assert actual_logs == expected_logs

    captured = capsys.readouterr()
    assert captured.out == "this is info level\n"
    assert captured.err == "warning!\nerror!!\n"


def test_civis_logger_set_to_debug_level(caplog, capsys):
    log = _get_test_logger(level=logging.DEBUG)
    caplog.set_level(log.level)

    log.debug("debug level")
    log.info("this is info level")
    log.warning("warning!")
    log.error("error!!")

    actual_logs = [(rec.levelname, rec.message) for rec in caplog.records]
    expected_logs = [
        ("DEBUG", "debug level"),
        ("INFO", "this is info level"),
        ("WARNING", "warning!"),
        ("ERROR", "error!!"),
    ]
    assert actual_logs == expected_logs

    captured = capsys.readouterr()
    assert captured.out == "debug level\nthis is info level\n"
    assert captured.err == "warning!\nerror!!\n"


def test_civis_logger_fmt_from_str(caplog, capsys):
    log = _get_test_logger(fmt="%(levelname)s:%(message)s")
    caplog.set_level(log.level)

    log.debug("debug level")
    log.info("this is info level")
    log.warning("warning!")
    log.error("error!!")

    actual_logs = [(rec.levelname, rec.message) for rec in caplog.records]
    expected_logs = [
        ("INFO", "this is info level"),
        ("WARNING", "warning!"),
        ("ERROR", "error!!"),
    ]
    assert actual_logs == expected_logs

    captured = capsys.readouterr()
    assert captured.out == "INFO:this is info level\n"
    assert captured.err == "WARNING:warning!\nERROR:error!!\n"


def test_civis_logger_fmt_from_formatter(caplog, capsys):
    fmt = logging.Formatter("%(levelname)s:%(message)s")
    log = _get_test_logger(fmt=fmt)
    caplog.set_level(log.level)

    log.debug("debug level")
    log.info("this is info level")
    log.warning("warning!")
    log.error("error!!")

    actual_logs = [(rec.levelname, rec.message) for rec in caplog.records]
    expected_logs = [
        ("INFO", "this is info level"),
        ("WARNING", "warning!"),
        ("ERROR", "error!!"),
    ]
    assert actual_logs == expected_logs

    captured = capsys.readouterr()
    assert captured.out == "INFO:this is info level\n"
    assert captured.err == "WARNING:warning!\nERROR:error!!\n"
