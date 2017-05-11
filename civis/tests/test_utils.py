from civis.compat import mock
from civis._utils import camel_to_snake, to_camelcase, maybe_get_random_name


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
