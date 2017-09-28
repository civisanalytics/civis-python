from requests import ConnectionError, ConnectTimeout
from civis.compat import mock
from civis._utils import camel_to_snake, to_camelcase, maybe_get_random_name, retry


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


class TestRetry:
    """Test class for retry decorator"""

    def setUp(self):
        self.counter = 0

    def test_no_retry_required(self):
        @retry(ConnectionError, retries=4, delay=0.1)
        def succeeds():
            self.counter += 1
            return 'success'

        test_result = succeeds()

        assert test_result == 'success'
        assert self.counter == 1

    def test_retries_once(self):
        @retry(ConnectionError, retries=4, delay=0.1)
        def fails_once():
            self.counter += 1
            if self.counter < 2:
                raise ConnectionError('failed')
            else:
                return 'success'

        test_result = fails_once()

        assert test_result == 'success'
        assert self.counter == 2

    def test_limit_is_reached(self):
        @retry(ConnectionError, retries=4, delay=0.1)
        def always_fails():
            self.counter += 1
            raise ConnectionError('failed')

        assert isinstance(always_fails(), ConnectionError)
        assert self.counter == 5

    def test_multiple_exception_types(self):
        @retry((ConnectionError, ConnectTimeout), retries=4, delay=0.1)
        def raise_multiple_exceptions():
            self.counter += 1
            if self.counter == 1:
                raise ConnectionError('one error')
            elif self.counter == 2:
                raise ConnectTimeout('another error')
            else:
                return 'success'

        test_result = raise_multiple_exceptions()

        assert test_result == 'success'
        assert self.counter == 3

    def test_unexpected_exception_does_not_retry(self):
        @retry(ConnectionError, retries=4, delay=0.1)
        def raise_unexpected_error():
            raise ValueError('unexpected error')

        assert isinstance(raise_unexpected_error(), ValueError)
