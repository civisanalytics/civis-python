from civis._camel_to_snake import camel_to_snake


def test_camel_to_snake():
    test_cases = [
        ("CAMELCase", "camel_case"),
        ("camelCase", "camel_case"),
        ("CamelCase", "camel_case"),
        ("c__amel", "c__amel"),
    ]
    for in_word, out_word in test_cases:
        assert camel_to_snake(in_word) == out_word
