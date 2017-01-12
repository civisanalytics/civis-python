import unittest
from unittest import mock
from unittest.mock import patch

import pytest

from civis.base import CivisJobFailure
try:
    from civis.pubnub import (SubscribableResult,
                              has_pubnub,
                              JobCompleteListener)
except ImportError:
    has_pubnub = False


class PubnubTests(unittest.TestCase):
    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    def test_listener_calls_callback_when_message_matches(self):
        match = mock.Mock()
        match.return_value = True
        callback = mock.Mock()
        listener = JobCompleteListener(match, callback)
        message = mock.Mock()
        message.message.return_value = 'test message'

        listener.message(None, message)
        match.assert_called_with(message.message)
        self.assertEqual(callback.call_count, 1)

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    def test_listener_does_not_call_callback(self):
        match = mock.Mock()
        match.return_value = False
        callback = mock.Mock()
        listener = JobCompleteListener(match, callback)
        message = mock.Mock()
        message.message.return_value = 'test message'

        listener.message(None, message)
        match.assert_called_with(message.message)
        self.assertEqual(callback.call_count, 0)

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_check_message(self, *mocks):
        result = SubscribableResult(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 1
            },
            'run': {
                'id': 20,
                'state': 'succeeded'
            }
        }
        self.assertTrue(result._check_message(message))

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_check_message_with_different_run_id(self, *mocks):
        result = SubscribableResult(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 2
            },
            'run': {
                'id': 20,
                'state': 'succeeded'
            }
        }
        self.assertFalse(result._check_message(message))

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_check_message_when_job_is_running(self, *mocks):
        result = SubscribableResult(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 1
            },
            'run': {
                'id': 20,
                'state': 'running'
            }
        }
        self.assertFalse(result._check_message(message))

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_set_api_result_poller(self, mock_subscribe):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        poller_result = mock.Mock()
        poller_result.state = 'succeeded'
        poller.return_value = poller_result

        result = SubscribableResult(poller, (1, 2))
        result._set_api_result()
        poller.assert_called_with(1, 2)
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_set_api_result_explicit_result(self, mock_subscribe):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'succeeded'

        result = SubscribableResult(poller, (1, 2))
        result._set_api_result(api_result)
        assert poller.call_count == 0
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @patch.object(SubscribableResult, '_subscribe')
    def test_set_api_result_failed(self, mock_subscribe):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'failed'

        result = SubscribableResult(poller, (1, 2))
        result._set_api_result(api_result)
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'
        with pytest.raises(CivisJobFailure):
            result.result()
