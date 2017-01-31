from unittest import mock

from civis import result


@mock.patch.object(result, 'has_pubnub', True)
@mock.patch.object(result, 'APIClient', mock.Mock())
@mock.patch.object(result, 'SubscribableResult')
@mock.patch.object(result, 'PollableResult')
def test_pubnub_available(mock_pollable, mock_subscribable):
    mock_poller = mock.Mock()
    out = result.make_platform_future(mock_poller, 11,
                                      polling_interval=29,
                                      api_key='secret')

    assert mock_pollable.call_count == 0
    mock_subscribable.assert_called_once_with(poller=mock_poller,
                                              poller_args=11,
                                              polling_interval=29,
                                              api_key='secret')
    assert out == mock_subscribable()


@mock.patch.object(result, 'has_pubnub', False)
@mock.patch.object(result, 'APIClient', mock.Mock())
@mock.patch.object(result, 'SubscribableResult')
@mock.patch.object(result, 'PollableResult')
def test_pubnub_not_available(mock_pollable, mock_subscribable):
    mock_poller = mock.Mock()
    out = result.make_platform_future(mock_poller, 11,
                                      polling_interval=29,
                                      api_key='secret')

    assert mock_subscribable.call_count == 0
    mock_pollable.assert_called_once_with(poller=mock_poller,
                                          poller_args=11,
                                          polling_interval=29,
                                          api_key='secret')
    assert out == mock_pollable()
