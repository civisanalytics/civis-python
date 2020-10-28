from unittest import mock

from requests import Request
from requests import ConnectionError, ConnectTimeout
from datetime import datetime
from math import floor

from civis._utils import camel_to_snake, to_camelcase, maybe_get_random_name
from civis._utils import retry
from civis._utils import retry_request

from civis.civis import RETRY_VERBS, RETRY_CODES, POST_RETRY_CODES

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


def test_io_no_retry():
    @retry(ConnectionError, retries=4, delay=0.1)
    def succeeds():
        counter['i'] += 1
        return 'success'

    counter = dict(i=0)
    test_result = succeeds()

    assert test_result == 'success'
    assert counter['i'] == 1


def test_io_retry_once():
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


def test_io_retry_limit_reached():
    @retry(ConnectionError, retries=4, delay=0.1)
    def always_fails():
        counter['i'] += 1
        raise ConnectionError('failed')

    counter = dict(i=0)
    pytest.raises(ConnectionError, always_fails)
    assert counter['i'] == 5


def test_io_retry_multiple_exceptions():
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


def test_io_retry_unexpected_exception():
    @retry(ConnectionError, retries=4, delay=0.1)
    def raise_unexpected_error():
        raise ValueError('unexpected error')

    pytest.raises(ValueError, raise_unexpected_error)


@mock.patch('civis._utils.open_session')
def test_no_retry_on_success(mock_session):
    expected_call_count = 0
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in RETRY_VERBS:
        expected_call_count += 1
        session_context.send.return_value.status_code = 200

        request_info = dict(
            params={'secondParameter': 'b', 'firstParameter': 'a'},
            json={},
            url='https://api.civisanalytics.com/wobble/wubble',
            method=verb
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request(verb, pre_request, session_context, 3)

        assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_no_retry_on_get_no_retry_failure(mock_session):
    expected_call_count = 0
    max_calls = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in RETRY_VERBS:
        expected_call_count += 1
        session_context.send.return_value.status_code = 403

        request_info = dict(
            params={'secondParameter': 'b', 'firstParameter': 'a'},
            json={},
            url='https://api.civisanalytics.com/wobble/wubble',
            method=verb
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request(verb, pre_request, session_context, max_calls)

        assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_retry_on_retry_eligible_failures(mock_session):
    expected_call_count = 0
    max_calls = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    for verb in RETRY_VERBS:
        for code in RETRY_CODES:
            expected_call_count += max_calls
            session_context.send.return_value.status_code = code

            request_info = dict(
                params={'secondParameter': 'b', 'firstParameter': 'a'},
                json={},
                url='https://api.civisanalytics.com/wobble/wubble',
                method=verb
            )

            request = Request(**request_info)
            pre_request = session_context.prepare_request(request)
            retry_request(verb, pre_request, session_context, max_calls)

            assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_retry_on_retry_eligible_failures_lowercase_verbs(mock_session):
    expected_call_count = 0
    max_calls = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    for verb in RETRY_VERBS:
        for code in RETRY_CODES:
            expected_call_count += max_calls
            session_context.send.return_value.status_code = code

            request_info = dict(
                params={'secondParameter': 'b', 'firstParameter': 'a'},
                json={},
                url='https://api.civisanalytics.com/wobble/wubble',
                method=verb.lower()
            )

            request = Request(**request_info)
            pre_request = session_context.prepare_request(request)
            retry_request(verb, pre_request, session_context, max_calls)

            assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_no_retry_on_post_success(mock_session):
    expected_call_count = 1
    max_calls = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    session_context.send.return_value.status_code = 200

    request_info = dict(
        params={'secondParameter': 'b', 'firstParameter': 'a'},
        json={},
        url='https://api.civisanalytics.com/wobble/wubble',
        method='POST'
    )
    request = Request(**request_info)
    pre_request = session_context.prepare_request(request)
    retry_request('post', pre_request, session_context, max_calls)

    assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_retry_on_retry_eligible_post_failures(mock_session):
    expected_call_count = 0
    max_calls = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for code in POST_RETRY_CODES:
        expected_call_count += max_calls
        session_context.send.return_value.status_code = code

        request_info = dict(
            params={'secondParameter': 'b', 'firstParameter': 'a'},
            json={},
            url='https://api.civisanalytics.com/wobble/wubble',
            method='POST'
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request('post', pre_request, session_context, max_calls)

        assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_no_retry_on_connection_error(mock_session):
    expected_call_count = 0
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in RETRY_VERBS:
        expected_call_count += 1

        request_info = dict(
            params={'secondParameter': 'b', 'firstParameter': 'a'},
            json={},
            url='https://api.civisanalytics.com/wobble/wubble',
            method=verb
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)

        session_context.send.side_effect = ConnectionError()
        try:
            retry_request(verb, pre_request, session_context, 3)
        except ConnectionError:
            pass

        assert session_context.send.call_count == expected_call_count


@mock.patch('civis._utils.open_session')
def test_retry_respect_retry_after_headers(mock_session):
    expected_call_count = 0
    max_calls = 3
    retry_after = 3
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    session_context.send.return_value.status_code = 429
    session_context.send.return_value.headers = {
        'Retry-After': str(retry_after)
    }

    for verb in ['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE', 'POST',
                 'head', 'trace', 'get', 'put', 'options', 'delete', 'post']:
        expected_call_count += max_calls

        request_info = dict(
            params={'secondParameter': 'b', 'firstParameter': 'a'},
            json={},
            url='https://api.civisanalytics.com/wobble/wubble',
            method=verb
        )

        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)

        start_time = datetime.now().timestamp()
        retry_request(verb, pre_request, session_context, max_calls)
        end_time = datetime.now().timestamp()
        duration = end_time - start_time

        assert session_context.send.call_count == expected_call_count
        assert floor(duration) == retry_after * (max_calls - 1)
