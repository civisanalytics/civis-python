from unittest import mock

from requests import Request
from requests import ConnectionError
from datetime import datetime
from math import floor

from civis._utils import maybe_get_random_name
from civis._utils import retry_request
from civis._utils import _RETRY_VERBS, _RETRY_CODES, _POST_RETRY_CODES


@mock.patch("civis._utils.uuid")
def test_maybe_random_name_random(mock_uuid):
    random_name = "11111"
    mock_uuid.uuid4.return_value = mock.Mock(hex=random_name)
    assert maybe_get_random_name(None) == random_name


def test_maybe_random_name_not_random():
    given_name = "22222"
    assert maybe_get_random_name(given_name) == given_name


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
        retry_request(verb, pre_request, session_context, 3)

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
        retry_request(verb, pre_request, session_context, max_calls)

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
            retry_request(verb, pre_request, session_context, max_calls)

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
            retry_request(verb, pre_request, session_context, max_calls)

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
    retry_request("post", pre_request, session_context, max_calls)

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
        retry_request("post", pre_request, session_context, max_calls)

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
            retry_request(verb, pre_request, session_context, 3)
        except ConnectionError:
            pass

        assert session_context.send.call_count == expected_call_count


def test_retry_respect_retry_after_headers():
    expected_call_count = 0
    max_calls = 2
    retry_after = 1
    api_response = {"key": "value"}
    mock_session = mock.MagicMock()
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response

    session_context.send.return_value.status_code = 429
    session_context.send.return_value.headers = {"Retry-After": str(retry_after)}

    for verb in [
        "HEAD",
        "TRACE",
        "GET",
        "PUT",
        "OPTIONS",
        "DELETE",
        "POST",
        "head",
        "trace",
        "get",
        "put",
        "options",
        "delete",
        "post",
    ]:
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
        retry_request(verb, pre_request, session_context, max_calls)
        end_time = datetime.now().timestamp()
        duration = end_time - start_time

        assert session_context.send.call_count == expected_call_count
        assert floor(duration) == retry_after * (max_calls - 1)
