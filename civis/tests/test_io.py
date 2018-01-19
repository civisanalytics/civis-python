from collections import OrderedDict
import json
import os
from six import StringIO, BytesIO
import tempfile
import zipfile

import pytest
import vcr

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

import civis
from civis.compat import mock, FileNotFoundError
from civis.response import Response
from civis.base import CivisAPIError
from civis.resources._resources import get_api_spec, generate_classes
from civis.tests.testcase import (CivisVCRTestCase,
                                  cassette_dir,
                                  POLL_INTERVAL)
from civis.tests import TEST_SPEC

api_import_str = 'civis.resources._resources.get_api_spec'
with open(TEST_SPEC) as f:
    civis_api_spec = json.load(f, object_pairs_hook=OrderedDict)


class MockAPIError(CivisAPIError):
    """A fake API error with only a status code"""
    def __init__(self, sc):
        self.status_code = sc


@mock.patch(api_import_str, return_value=civis_api_spec)
class ImportTests(CivisVCRTestCase):
    # Note that all functions tested here should use a
    # `polling_interval=POLL_INTERVAL` input. This lets us use
    # sensible polling intervals when recording, but speed through
    # the calls in the VCR cassette when testing later.

    @classmethod
    def setUpClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    def tearDownClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def setup_class(cls, *mocks):
        setup_vcr = vcr.VCR(filter_headers=['Authorization'])
        setup_cassette = os.path.join(cassette_dir(), 'io_setup.yml')
        with setup_vcr.use_cassette(setup_cassette):
            # create a file
            buf = StringIO()
            buf.write('a,b,c\n1,2,3')
            buf.seek(0)
            file_id = civis.io.file_to_civis(buf, 'somename')
            cls.file_id = file_id

            # create the table. assumes this function works.
            sql = """
                DROP TABLE IF EXISTS scratch.api_client_test_fixture;

                CREATE TABLE scratch.api_client_test_fixture (
                    a int,
                    b int,
                    c int
                );

                INSERT INTO scratch.api_client_test_fixture
                VALUES (1,2,3);
            """
            res = civis.io.query_civis(sql, 'redshift-general',
                                       polling_interval=POLL_INTERVAL)
            res.result()  # block

            # create an export to check get_url. also tests export_csv
            with tempfile.NamedTemporaryFile() as tmp:
                sql = "SELECT * FROM scratch.api_client_test_fixture"
                database = 'redshift-general'
                result = civis.io.civis_to_csv(tmp.name, sql, database,
                                               polling_interval=POLL_INTERVAL)
                result = result.result()
                cls.export_url = result['output'][0]['path']
                assert result.state == 'succeeded'

            cls.export_job_id = result.sql_id

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_zip_member_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(tmp.name, 'a,b,c\n1,2,3')
                zip_member = zip_file.namelist()[0]
                with zip_file.open(zip_member) as zip_member_buf:
                    result = civis.io.file_to_civis(zip_member_buf, zip_member)

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_text_file_to_civis(self, *mocks):
        buf = StringIO()
        buf.write('a,b,c\n1,2,3')
        buf.seek(0)
        result = civis.io.file_to_civis(buf, 'somename')

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_bytes_file_to_civis(self, *mocks):
        buf = BytesIO()
        buf.write(b'a,b,c\n1,2,3')
        buf.seek(0)
        result = civis.io.file_to_civis(buf, 'somename')

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_large_file_to_civis(self, *mocks):
        curr_size = civis.io._files.MIN_MULTIPART_SIZE
        civis.io._files.MIN_MULTIPART_SIZE = 1
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'a,b,c\n1,2,3')
            tmp.flush()
            tmp.seek(0)
            result = civis.io.file_to_civis(tmp, tmp.name)

            civis.io._files.MIN_MULTIPART_SIZE = curr_size

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_file(self, *mocks):
        buf = BytesIO()
        civis.io.civis_to_file(self.file_id, buf)
        buf.seek(0)
        assert buf.read() == b'a,b,c\n1,2,3'

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_csv_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'a,b,c\n1,2,3')
            tmp.flush()

            table = "scratch.api_client_test_fixture"
            database = 'redshift-general'
            result = civis.io.csv_to_civis(tmp.name, database, table,
                                           existing_table_rows='truncate',
                                           polling_interval=POLL_INTERVAL)
            result = result.result()  # block until done

        assert isinstance(result.id, int)
        assert result.state == 'succeeded'

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_file_to_table(self, *mocks):
        table = "scratch.api_client_test_fixture"
        database = 'redshift-general'
        result = civis.io.civis_file_to_table(self.file_id, database, table,
                                              existing_table_rows='truncate',
                                              polling_interval=POLL_INTERVAL)
        result = result.result()  # block until done

        assert isinstance(result.id, int)
        assert result.state == 'succeeded'

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_pandas(self, *mocks):
        expected = pd.DataFrame([[1, 2, 3]], columns=['a', 'b', 'c'])
        df = civis.io.read_civis('scratch.api_client_test_fixture',
                                 'redshift-general', use_pandas=True,
                                 polling_interval=POLL_INTERVAL)
        assert df.equals(expected)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_no_pandas(self, *mocks):
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis('scratch.api_client_test_fixture',
                                   'redshift-general', use_pandas=False,
                                   polling_interval=POLL_INTERVAL)
        assert data == expected

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_sql(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis_sql(sql, 'redshift-general',
                                       use_pandas=False,
                                       polling_interval=POLL_INTERVAL)
        assert data == expected

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_dataframe_to_civis(self, *mocks):
        df = pd.DataFrame([['1', '2', '3']], columns=['a', 'b', 'c'])
        result = civis.io.dataframe_to_civis(df, 'redshift-general',
                                             'scratch.api_client_test_fixture',
                                             existing_table_rows='truncate',
                                             polling_interval=POLL_INTERVAL)
        result = result.result()
        assert result.state == 'succeeded'

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_dataframe_to_civis_with_index(self, *mocks):
        df = pd.DataFrame([['1', '2', '3']], columns=['a', 'b', 'c'])
        result = civis.io.dataframe_to_civis(df, 'redshift-general',
                                             'scratch.api_client_test_fixture',
                                             existing_table_rows='truncate',
                                             polling_interval=POLL_INTERVAL,
                                             index=True)
        result = result.result()
        assert result.state == 'succeeded'

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_multifile_csv(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        result = civis.io.civis_to_multifile_csv(
            sql, database='redshift-general', polling_interval=POLL_INTERVAL)
        assert set(result.keys()) == {'entries', 'query', 'header',
                                      'delimiter', 'compression', 'unquoted'}
        assert result['query'] == sql
        assert result['header'] == ['a', 'b', 'c']
        assert isinstance(result['entries'], list)
        assert set(result['entries'][0].keys()) == {'id', 'name', 'size',
                                                    'url', 'url_signed'}
        assert result['entries'][0]['url_signed'].startswith('https://civis-'
                                                             'console.s3.'
                                                             'amazonaws.com/')

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_transfer_table(self, *mocks):
        result = civis.io.transfer_table('redshift-general', 'redshift-test',
                                         'scratch.api_client_test_fixture',
                                         'scratch.api_client_test_fixture',
                                         polling_interval=POLL_INTERVAL)
        result = result.result()
        assert result.state == 'succeeded'

        # check for side effect
        sql = 'select * from scratch.api_client_test_fixture'
        result = civis.io.query_civis(sql, 'redshift-test',
                                      polling_interval=POLL_INTERVAL).result()
        assert result.state == 'succeeded'

    def test_get_sql_select(self, *mocks):
        x = "select * from schema.table"
        y = "select a, b, c from schema.table"
        table = "schema.table"
        assert civis.io._tables._get_sql_select(table) == x
        assert civis.io._tables._get_sql_select(table, ['a', 'b', 'c']) == y
        with pytest.raises(TypeError):
            civis.io._tables._get_sql_select(table, "column_a")

    def test_download_file(self, *mocks):
        expected = '"1","2","3"\n'
        with tempfile.NamedTemporaryFile() as tmp:
            civis.io._tables._download_file(self.export_url, tmp.name,
                                            b'', 'none')
            with open(tmp.name, "r") as f:
                data = f.read()
        assert data == expected


def test_file_id_from_run_output_exact():
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = \
        [Response({'name': 'spam', 'object_id': 2013,
                   'object_type': 'File'})]

    fid = civis.io.file_id_from_run_output('spam', 17, 13, client=m_client)
    assert fid == 2013


def test_file_id_from_run_output_approximate():
    # Test fuzzy name matching
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = \
        [Response({'name': 'spam.csv.gz', 'object_id': 2013,
                   'object_type': 'File'})]

    fid = civis.io.file_id_from_run_output('^spam', 17, 13, regex=True,
                                           client=m_client)
    assert fid == 2013


def test_file_id_from_run_output_approximate_multiple():
    # Fuzzy name matching with muliple matches should return the first
    m_cl = mock.Mock()
    m_cl.scripts.list_containers_runs_outputs.return_value = [
        Response({'name': 'spam.csv.gz', 'object_id': 2013,
                  'object_type': 'File'}),
        Response({'name': 'eggs.csv.gz', 'object_id': 2014,
                  'object_type': 'File'})]

    fid = civis.io.file_id_from_run_output('.csv', 17, 13, regex=True,
                                           client=m_cl)
    assert fid == 2013


def test_file_id_from_run_output_no_file():
    # Get an IOError if we request a file which doesn't exist
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = [
        Response({'name': 'spam', 'object_id': 2013,
                  'object_type': 'File'})]

    with pytest.raises(FileNotFoundError) as err:
        civis.io.file_id_from_run_output('eggs', 17, 13, client=m_client)
    assert 'not an output' in str(err.value)


def test_file_id_from_run_output_no_run():
    # Get an IOError if we request a file from a run which doesn't exist
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.side_effect =\
        MockAPIError(404)  # Mock a run which doesn't exist

    with pytest.raises(IOError) as err:
        civis.io.file_id_from_run_output('name', 17, 13, client=m_client)
    assert 'could not find job/run id 17/13' in str(err.value).lower()


def test_file_id_from_run_output_platform_error():
    # Make sure we don't swallow real Platform errors
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.side_effect =\
        MockAPIError(500)  # Mock a platform error
    with pytest.raises(CivisAPIError):
        civis.io.file_id_from_run_output('name', 17, 13, client=m_client)


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer():
    m_client = mock.Mock()
    m_client.files.get.return_value = Response({'name': 'spam.csv',
                                                'file_url': 'url'})
    with mock.patch.object(civis.io._files.pd, 'read_csv') as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='infer', client=m_client)
        assert mock_read_csv.called_once_with(121, compression='infer')


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer_gzip():
    m_client = mock.Mock()
    m_client.files.get.return_value = Response({'name': 'spam.csv.gz',
                                                'file_url': 'url'})
    with mock.patch.object(civis.io._files.pd, 'read_csv') as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='infer', client=m_client)
        assert mock_read_csv.called_once_with(121, compression='gzip')


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_kwargs():
    m_client = mock.Mock()
    m_client.files.get.return_value = Response({'name': 'spam.csv',
                                                'file_url': 'url'})
    with mock.patch.object(civis.io._files.pd, 'read_csv') as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='special', client=m_client,
                                   delimiter='|', nrows=10)
        assert mock_read_csv.called_once_with(121, compression='special',
                                              delimiter='|', nrows=10)


@mock.patch.object(civis.io._files, 'civis_to_file', autospec=True)
def test_load_json(mock_c2f):
    obj = {'spam': 'eggs'}

    def _dump_json(file_id, buf, *args, **kwargs):
        buf.write(json.dumps(obj).encode())
    mock_c2f.side_effect = _dump_json
    out = civis.io.file_to_json(13, client=mock.Mock())
    assert out == obj


@mock.patch('civis.io._files._civis_to_file')
@mock.patch('%s.open' % __name__, create=True)
def test_civis_to_file_local(mock_open, mock_civis_to_file_helper):
    # Test that passing a path to civis_to_file opens a file.
    civis.io.civis_to_file(123, "foo")
    mock_open.return_value = mock_file = mock.Mock()
    assert mock_open.called_once_with("foo", "wb")
    assert mock_civis_to_file_helper.called_once_with(123, mock_file)


@mock.patch('civis.io._files._file_to_civis')
@mock.patch('%s.open' % __name__, create=True)
def test_file_to_civis(mock_open, mock_file_to_civis_helper):
    # Test that passing a path to file_to_civis opens a file.
    civis.io.file_to_civis("foo", "foo_name")
    mock_open.return_value = mock_file = mock.Mock()
    assert mock_open.called_once_with("foo", "rb")
    assert mock_file_to_civis_helper.called_once_with(
        "foo", "foo_name", mock_file)
