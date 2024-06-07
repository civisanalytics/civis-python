# The `camel_to_snake` function is used in multiple modules.
# To avoid creating import overhead, it's defined in a separate module here
# as opposed to in a module that itself has a fair amount of overhead.

import re


UNDERSCORER1 = re.compile(r"(.)([A-Z][a-z]+)")
UNDERSCORER2 = re.compile("([a-z0-9])([A-Z])")


def camel_to_snake(word):
    # https://gist.github.com/jaytaylor/3660565
    word = UNDERSCORER1.sub(r"\1_\2", word)
    return UNDERSCORER2.sub(r"\1_\2", word).lower()
