from functools import wraps
import os

from vcr import persist
from vcr.serialize import CASSETTE_FORMAT_VERSION
from vcr.serializers import compat
from vcr_unittest import VCRTestCase


def conditionally_patch(target, *args, **kwargs):
    from unittest.mock import patch
    if os.getenv('GENERATE_TESTS'):
        def pass_func(func):
            wraps(func)
            return func
        return pass_func
    else:
        def decorated_func(func):
            return patch(target, *args, **kwargs)(func)
        return decorated_func


def cassette_dir():
    testdir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(testdir, 'cassettes')


# monkey patches the persist.serialize function in vcrpy
# https://github.com/kevin1024/vcrpy/issues/273


def github_serialize(cassette_dict, serializer):
    interactions = ([{
        'request': compat.convert_to_unicode(request._to_dict()),
        'response': compat.convert_to_unicode(response),
    } for request, response in zip(
        cassette_dict['requests'],
        cassette_dict['responses'],
    )])
    data = {
        'version': CASSETTE_FORMAT_VERSION,
        'interactions': interactions,
        'recorded_with': 'VCR'
    }
    return serializer.serialize(data)


persist.serialize = github_serialize


class CivisVCRTestCase(VCRTestCase):

    def _get_vcr_kwargs(self):
        return {
            'filter_headers': ['Authorization'],
            'record_mode': 'once',
            'path_transformer': lambda x: x.replace('yaml', 'yml')
        }
