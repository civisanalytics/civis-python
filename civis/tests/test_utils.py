from requests import ConnectionError, ConnectTimeout

from civis.compat import mock
from civis._utils import camel_to_snake, to_camelcase, maybe_get_random_name
from civis._utils import retry

import pytest


def test_camel_to_snake():
    test_cases = [
        ('CAMELCase', 'camel_case'),
        ('camelCase', 'camel_case'),
        ('CamelCase', 'camel_case'),
        ('c__amel', 'c__amel'),
    ]
    for in_word, out_word in test_cases:
        assert camel_to_snake(in_word) == out_word


def test_tocamlecase():
    test_cases = [
        ('snake_case', 'SnakeCase'),
        ('Snake_Case', 'SnakeCase'),
        ('snakecase', 'Snakecase')
    ]
    for in_word, out_word in test_cases:
        assert to_camelcase(in_word) == out_word


@mock.patch('civis._utils.uuid')
def test_maybe_random_name_random(mock_uuid):
    random_name = '11111'
    mock_uuid.uuid4.return_value = mock.Mock(hex=random_name)
    assert maybe_get_random_name(None) == random_name


def test_maybe_random_name_not_random():
    given_name = '22222'
    assert maybe_get_random_name(given_name) == given_name


def test_no_retry():
    @retry(ConnectionError, retries=4, delay=0.1)
    def succeeds():
        counter['i'] += 1
        return 'success'

    counter = dict(i=0)
    test_result = succeeds()

    assert test_result == 'success'
    assert counter['i'] == 1


def test_retry_once():
    @retry(ConnectionError, retries=4, delay=0.1)
    def fails_once():
        counter['i'] += 1
        if counter['i'] < 2:
            raise ConnectionError('failed')
        else:
            return 'success'

    counter = dict(i=0)
    test_result = fails_once()

    assert test_result == 'success'
    assert counter['i'] == 2


def test_retry_limit_reached():
    @retry(ConnectionError, retries=4, delay=0.1)
    def always_fails():
        counter['i'] += 1
        raise ConnectionError('failed')

    counter = dict(i=0)
    pytest.raises(ConnectionError, always_fails)
    assert counter['i'] == 5


def test_retry_multiple_exceptions():
    @retry((ConnectionError, ConnectTimeout), retries=4, delay=0.1)
    def raise_multiple_exceptions():
        counter['i'] += 1
        if counter['i'] == 1:
            raise ConnectionError('one error')
        elif counter['i'] == 2:
            raise ConnectTimeout('another error')
        else:
            return 'success'

    counter = dict(i=0)
    test_result = raise_multiple_exceptions()

    assert test_result == 'success'
    assert counter['i'] == 3


def test_retry_unexpected_exception():
    @retry(ConnectionError, retries=4, delay=0.1)
    def raise_unexpected_error():
        raise ValueError('unexpected error')

    pytest.raises(ValueError, raise_unexpected_error)
