import copy
import concurrent.futures as cf
from datetime import datetime
from math import floor
from unittest import mock

import tenacity
from requests import Request
from requests import ConnectionError

from civis._utils import retry_request, DEFAULT_RETRYING
from civis._utils import _RETRY_VERBS, _RETRY_CODES, _POST_RETRY_CODES


def _get_retrying(retries: int):
    retrying = copy.copy(DEFAULT_RETRYING)
    stop = tenacity.stop_after_delay(600) | tenacity.stop_after_attempt(retries)
    retrying.stop = stop
    return retrying


def test_no_retry_on_success():
    expected_call_count = 0
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in _RETRY_VERBS:
        expected_call_count += 1
        session_context.send.return_value.status_code = 200

        request_info = dict(
            params={"secondParameter": "b", "firstParameter": "a"},
            json={},
            url="https://api.civisanalytics.com/wobble/wubble",
            method=verb,
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request(verb, pre_request, session_context, _get_retrying(3))

        assert session_context.send.call_count == expected_call_count


def test_no_retry_on_get_no_retry_failure():
    expected_call_count = 0
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in _RETRY_VERBS:
        expected_call_count += 1
        session_context.send.return_value.status_code = 403

        request_info = dict(
            params={"secondParameter": "b", "firstParameter": "a"},
            json={},
            url="https://api.civisanalytics.com/wobble/wubble",
            method=verb,
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request(verb, pre_request, session_context, _get_retrying(max_calls))

        assert session_context.send.call_count == expected_call_count


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_retry_on_retry_eligible_failures(m_sleep):
    expected_call_count = 0
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    for verb in _RETRY_VERBS:
        for code in _RETRY_CODES:
            expected_call_count += max_calls
            session_context.send.return_value.status_code = code

            request_info = dict(
                params={"secondParameter": "b", "firstParameter": "a"},
                json={},
                url="https://api.civisanalytics.com/wobble/wubble",
                method=verb,
            )

            request = Request(**request_info)
            pre_request = session_context.prepare_request(request)
            retry_request(verb, pre_request, session_context, _get_retrying(max_calls))

            assert session_context.send.call_count == expected_call_count


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_retry_on_retry_eligible_failures_lowercase_verbs(m_sleep):
    expected_call_count = 0
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    for verb in _RETRY_VERBS:
        for code in _RETRY_CODES:
            expected_call_count += max_calls
            session_context.send.return_value.status_code = code

            request_info = dict(
                params={"secondParameter": "b", "firstParameter": "a"},
                json={},
                url="https://api.civisanalytics.com/wobble/wubble",
                method=verb.lower(),
            )

            request = Request(**request_info)
            pre_request = session_context.prepare_request(request)
            retry_request(verb, pre_request, session_context, _get_retrying(max_calls))

            assert session_context.send.call_count == expected_call_count


def test_no_retry_on_post_success():
    expected_call_count = 1
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    session_context.send.return_value.status_code = 200

    request_info = dict(
        params={"secondParameter": "b", "firstParameter": "a"},
        json={},
        url="https://api.civisanalytics.com/wobble/wubble",
        method="POST",
    )
    request = Request(**request_info)
    pre_request = session_context.prepare_request(request)
    retry_request("post", pre_request, session_context, _get_retrying(max_calls))

    assert session_context.send.call_count == expected_call_count


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_retry_on_retry_eligible_post_failures(m_sleep):
    expected_call_count = 0
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for code in _POST_RETRY_CODES:
        expected_call_count += max_calls
        session_context.send.return_value.status_code = code

        request_info = dict(
            params={"secondParameter": "b", "firstParameter": "a"},
            json={},
            url="https://api.civisanalytics.com/wobble/wubble",
            method="POST",
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)
        retry_request("post", pre_request, session_context, _get_retrying(max_calls))

        assert session_context.send.call_count == expected_call_count


def test_no_retry_on_connection_error():
    expected_call_count = 0
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    for verb in _RETRY_VERBS:
        expected_call_count += 1

        request_info = dict(
            params={"secondParameter": "b", "firstParameter": "a"},
            json={},
            url="https://api.civisanalytics.com/wobble/wubble",
            method=verb,
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)

        session_context.send.side_effect = ConnectionError()
        try:
            retry_request(verb, pre_request, session_context, _get_retrying(3))
        except ConnectionError:
            pass

        assert session_context.send.call_count == expected_call_count


def _retry_respect_retry_after_headers(verb):
    expected_call_count = 0
    max_calls = 2
    retry_after = 1
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    session_context.send.return_value.status_code = 429
    session_context.send.return_value.headers = {"Retry-After": str(retry_after)}

    expected_call_count += max_calls

    request_info = dict(
        params={"secondParameter": "b", "firstParameter": "a"},
        json={},
        url="https://api.civisanalytics.com/wobble/wubble",
        method=verb,
    )

    request = Request(**request_info)
    pre_request = session_context.prepare_request(request)

    start_time = datetime.now().timestamp()
    retry_request(verb, pre_request, session_context, _get_retrying(max_calls))
    end_time = datetime.now().timestamp()
    duration = end_time - start_time

    assert session_context.send.call_count == expected_call_count
    assert floor(duration) == retry_after * (max_calls - 1)


def test_retry_respect_retry_after_headers():
    verbs = ["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE", "POST"]
    all_verbs = [v for v in (verbs + [v.lower() for v in verbs])]
    with cf.ThreadPoolExecutor() as executor:
        executor.map(_retry_respect_retry_after_headers, all_verbs)


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_no_recursion_on_reused_retrying_without_retry_after(m_sleep):
    """Test that reusing the same retrying object doesn't cause recursion.

    This simulates the real-world scenario where an APIClient reuses the same
    tenacity.Retrying instance across multiple API calls. Without proper
    guards, the wait strategy gets wrapped repeatedly, causing infinite
    recursion when Retry-After header is missing.
    """
    max_calls = 3
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    session_context.send.return_value.status_code = 429
    # No Retry-After header - this triggers the fallback behavior
    session_context.send.return_value.headers = {}

    # Create a single retrying instance that will be reused
    retrying = _get_retrying(max_calls)

    # Simulate multiple API calls using the same retrying object
    # In the real SDK, this happens when APIClient._retrying is reused
    # We need enough calls to create a deep nesting that exceeds recursion limit
    for i in range(1000):  # 1000 consecutive API calls to trigger deep recursion
        request_info = dict(
            params={"call_number": str(i)},
            json={},
            url="https://api.civisanalytics.com/test",
            method="GET",
        )
        request = Request(**request_info)
        pre_request = session_context.prepare_request(request)

        # This should not cause recursion even after multiple calls
        retry_request("GET", pre_request, session_context, retrying)

    # Verify that all calls completed without recursion error
    # Each call should have attempted max_calls times
    assert session_context.send.call_count == max_calls * 1000
