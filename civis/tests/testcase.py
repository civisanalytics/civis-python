import os

from vcr import persist
from vcr.serialize import CASSETTE_FORMAT_VERSION
from vcr.serializers import compat
from vcr_unittest import VCRTestCase

# The "GENERATE_TEST" environment variable indicates that
# we're recording new cassettes.
if os.environ.get('GENERATE_TESTS'):
    # Use default polling intervals if generating new tests.
    POLL_INTERVAL = None
else:
    # Speed through calls in pre-recorded VCR cassettes.
    POLL_INTERVAL = 0.00001


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
