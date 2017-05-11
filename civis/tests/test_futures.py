import os
import json
from collections import OrderedDict

import pytest

from civis.base import CivisJobFailure
from civis.compat import mock
from civis.resources._resources import get_api_spec, generate_classes
try:
    from civis.futures import (CivisFuture,
                               has_pubnub,
                               JobCompleteListener,
                               _LONG_POLLING_INTERVAL)
    from pubnub.enums import PNStatusCategory
except ImportError:
    has_pubnub = False

from civis.tests.testcase import CivisVCRTestCase

api_import_str = 'civis.resources._resources.get_api_spec'
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(THIS_DIR, "civis_api_spec.json")) as f:
    civis_api_spec_base = json.load(f, object_pairs_hook=OrderedDict)

with open(os.path.join(THIS_DIR, "civis_api_spec_channels.json")) as f:
    civis_api_spec_channels = json.load(f, object_pairs_hook=OrderedDict)


def clear_lru_cache():
    # LRU cache persists between tests so these caches need to be cleared
    # when different api specs are used in different test cases
    get_api_spec.cache_clear()
    generate_classes.cache_clear()


def setup_listener_status_mocks(status_category):
    match = mock.Mock()
    callback = mock.Mock()
    disconnect = mock.Mock()
    listener = JobCompleteListener(match, callback, disconnect)
    status = mock.Mock()
    status.category = status_category
    return match, callback, disconnect, listener, status


class CivisFutureTests(CivisVCRTestCase):

    @classmethod
    def setUpClass(cls):
        clear_lru_cache()

    @classmethod
    def tearDownClass(cls):
        clear_lru_cache()

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
    def test_listener_calls_disconnect_callback_when_status_disconnect(self):
        disconnect_categories = [
            PNStatusCategory.PNTimeoutCategory,
            PNStatusCategory.PNNetworkIssuesCategory,
            PNStatusCategory.PNUnexpectedDisconnectCategory,
        ]
        for category in disconnect_categories:
            mocks = setup_listener_status_mocks(category)
            _, _, disconnect, listener, status = mocks
            listener.status(None, status)
            assert disconnect.call_count == 1

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    def test_listener_does_note_call_disconnect_callback_on_other_status(self):
        nondisconnect_categories = [
            PNStatusCategory.PNAcknowledgmentCategory,
            PNStatusCategory.PNConnectedCategory,
            PNStatusCategory.PNReconnectedCategory,
        ]
        for category in nondisconnect_categories:
            mocks = setup_listener_status_mocks(category)
            _, _, disconnect, listener, status = mocks
            listener.status(None, status)
            assert disconnect.call_count == 0

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
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
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message_with_different_run_id(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
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
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message_when_job_is_running(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
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
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_set_api_result_result_succeeded(self, mock_subscribe, mock_api):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'succeeded'

        result = CivisFuture(poller, (1, 2))
        result._set_api_result(api_result)
        assert poller.call_count == 0
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_set_api_result_failed(self, mock_subscribe, mock_api):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'failed'

        result = CivisFuture(poller, (1, 2))
        result._set_api_result(api_result)
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'
        with pytest.raises(CivisJobFailure):
            result.result()

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x,
                             (1, 20))
        future._pubnub.get_subscribed_channels.return_value = [1]
        assert future.subscribed is True

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_no_subscription(self, *mocks):
        future = CivisFuture(lambda x: x,
                             (1, 20))
        future._pubnub.get_subscribed_channels.return_value = []
        assert future.subscribed is False

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_base)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_no_channels(self, *mocks):
        clear_lru_cache()
        future = CivisFuture(lambda x: x,
                             (1, 20))
        assert future.subscribed is False
        clear_lru_cache()

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_overwrite_polling_interval_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x, (1, 20))
        assert future.polling_interval == _LONG_POLLING_INTERVAL
        assert hasattr(future, '_pubnub')

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_explicit_polling_interval_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x, (1, 20), polling_interval=5)
        assert future.polling_interval == 5
        assert hasattr(future, '_pubnub')

    @pytest.mark.skipif(not has_pubnub, reason="pubnub not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec_base)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_polling_interval(self, *mocks):
        # This tests the fallback to polling when channels is not available.
        # It uses a different api spec than the other tests so it
        # should clear the cached values before and after
        clear_lru_cache()

        polling_interval = 30
        future = CivisFuture(lambda x: x,
                             (1, 20),
                             polling_interval=polling_interval)
        assert future.polling_interval == polling_interval
        assert hasattr(future, '_pubnub') is False

        clear_lru_cache()
