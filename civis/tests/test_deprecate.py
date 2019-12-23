from civis import _deprecation

import pytest


def adder(param1, param2=0, param3=0):
    return param1 + param2 + param3


def test_deprecate_kwarg():
    # Verify that we get a warning if the deprecated parameter is
    # used as a keyword argument.
    decorated_func = _deprecation.deprecate_param('v2.0.0', 'param2')(adder)

    with pytest.warns(FutureWarning) as record:
        output = decorated_func(1, param2=3, param3=5)

    assert output == 9, "The function should still give the expected output."
    assert len(record) == 1, "Only one warning should be raised."
    assert "v2.0.0" in record[0].message.args[0], \
        "The warning should mention the removal version."
    assert "param2" in record[0].message.args[0], \
        "The warning should mention the deprecated parameter."
    assert __name__ + ".adder" in record[0].message.args[0], \
        "The warning should mention the function name."


def test_deprecate_multiple_kwarg():
    # Verify that we get a warning if the deprecated parameter is
    # used as a keyword argument.
    decorated_func = _deprecation.deprecate_param(
        'v2.0.0', 'param2', 'param3')(adder)

    with pytest.warns(FutureWarning) as record:
        output = decorated_func(1, param2=3, param3=5)

    assert output == 9, "The function should still give the expected output."
    assert len(record) == 1, "Only one warning should be raised."
    assert "v2.0.0" in record[0].message.args[0], \
        "The warning should mention the removal version."
    assert "param2" in record[0].message.args[0], \
        "The warning should mention the first deprecated parameter."
    assert "param3" in record[0].message.args[0], \
        "The warning should mention the second deprecated parameter."
    assert __name__ + ".adder" in record[0].message.args[0], \
        "The warning should mention the function name."


def test_deprecate_pos_arg():
    # Verify that we get a warning if the deprecated parameter is
    # used as a positional argument.
    decorated_func = _deprecation.deprecate_param('v2.0.0', 'param2')(adder)

    with pytest.warns(FutureWarning) as record:
        output = decorated_func(1, 3, 5)

    assert output == 9, "The function should still give the expected output."
    assert len(record) == 1, "Only one warning should be raised."
    assert "v2.0.0" in record[0].message.args[0], \
        "The warning should mention the removal version."
    assert "param2" in record[0].message.args[0], \
        "The warning should mention the deprecated parameter."
    assert __name__ + ".adder" in record[0].message.args[0], \
        "The warning should mention the function name."


def test_deprecate_multiple_pos_arg():
    # Verify that we get a warning if the deprecated parameter is
    # used as a positional argument.
    decorated_func = _deprecation.deprecate_param(
        'v2.0.0', 'param2', 'param3')(adder)

    with pytest.warns(FutureWarning) as record:
        output = decorated_func(1, 3, 5)

    assert output == 9, "The function should still give the expected output."
    assert len(record) == 1, "Only one warning should be raised."
    assert "v2.0.0" in record[0].message.args[0], \
        "The warning should mention the removal version."
    assert "param2" in record[0].message.args[0], \
        "The warning should mention the first deprecated parameter."
    assert "param3" in record[0].message.args[0], \
        "The warning should mention the second deprecated parameter."
    assert __name__ + ".adder" in record[0].message.args[0], \
        "The warning should mention the function name."


def test_deprecate_no_warning():
    # Verify that we don't see a warning if we don't use the
    # deprecated parameter.
    decorated_func = _deprecation.deprecate_param('v2.0.0', 'param2')(adder)

    with pytest.warns(None) as record:
        output = decorated_func(1, param3=5)

    assert output == 6, "The function should still give the expected output."
    assert len(record) == 0, "No warnings should be raised."
