from collections import OrderedDict
import io
import json
import os
from six import StringIO, BytesIO
import zipfile

import pytest
import requests
import vcr

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

import civis
from civis.io import _files
from civis.compat import mock, FileNotFoundError, TemporaryDirectory
from civis.response import Response
from civis.base import CivisAPIError, CivisImportError, EmptyResultError
from civis.resources._resources import get_api_spec, generate_classes
from civis.tests.testcase import (CivisVCRTestCase,
                                  cassette_dir,
                                  POLL_INTERVAL)
from civis.tests import TEST_SPEC
from civis.tests.mocks import create_client_mock

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

    @pytest.fixture(autouse=True)
    def client_mock(self):
        self.mock_client = create_client_mock()

    @classmethod
    def tearDownClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def setUpClass(cls, *mocks):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

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
            with TemporaryDirectory() as temp_dir:
                fname = os.path.join(temp_dir, 'tempfile')
                sql = "SELECT * FROM scratch.api_client_test_fixture"
                database = 'redshift-general'
                result = civis.io.civis_to_csv(fname, sql, database,
                                               polling_interval=POLL_INTERVAL)
                result = result.result()
                cls.export_url = result['output'][0]['path']
                assert result.state == 'succeeded'

            cls.export_job_id = result.sql_id

    @pytest.mark.file_to_civis
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_zip_member_to_civis(self, *mocks):
        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, 'tempfile')
            with zipfile.ZipFile(fname, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                archive_name = 'archive_name'
                zip_file.writestr(archive_name, 'a,b,c\n1,2,3')
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

    @pytest.mark.file_to_civis
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_bytes_file_to_civis(self, *mocks):
        buf = BytesIO()
        buf.write(b'a,b,c\n1,2,3')
        buf.seek(0)
        result = civis.io.file_to_civis(buf, 'somename')

        assert isinstance(result, int)

    @pytest.mark.file_to_civis
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_large_file_to_civis(self, *mocks):
        curr_size = civis.io._files.MIN_MULTIPART_SIZE
        civis.io._files.MIN_MULTIPART_SIZE = 1
        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, 'tempfile')
            with open(fname, 'w+b') as tmp:
                tmp.write(b'a,b,c\n1,2,3')
            with open(fname, 'r+b') as tmp:
                result = civis.io.file_to_civis(tmp, fname)

            civis.io._files.MIN_MULTIPART_SIZE = curr_size

        assert isinstance(result, int)

    @pytest.mark.civis_to_file
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_file(self, *mocks):
        buf = BytesIO()
        civis.io.civis_to_file(self.file_id, buf)
        buf.seek(0)
        assert buf.read() == b'a,b,c\n1,2,3'

    @pytest.mark.csv_to_civis
    @mock.patch('civis.io._tables.file_to_civis')
    @mock.patch('civis.io._tables.civis_file_to_table')
    def test_csv_to_civis(self, m_civis_file_to_table, m_file_to_civis,
                          _m_get_api_spec):
        mock_file_id = 42
        m_file_to_civis.return_value = mock_file_id
        mock_future = mock.create_autospec(civis.futures.CivisFuture,
                                           spec_set=True)
        m_civis_file_to_table.return_value = mock_future
        table = "scratch.api_client_test_fixture"
        database = 'redshift-general'

        fname = 'a/tempfile'

        with mock.patch.object(civis.io._tables, 'open',
                               mock.mock_open(read_data='some,test,data'),
                               create=True) as m_open:

            result = civis.io.csv_to_civis(fname, database, table,
                                           client=self.mock_client,
                                           existing_table_rows='truncate')

            m_file_to_civis.assert_called_once_with(m_open.return_value,
                                                    'tempfile',
                                                    client=self.mock_client)

        assert result == mock_future

        m_civis_file_to_table.assert_called_once_with(
            mock_file_id, database, table,
            client=self.mock_client,
            max_errors=None,
            existing_table_rows='truncate',
            diststyle=None, distkey=None,
            sortkey1=None, sortkey2=None,
            delimiter=",", headers=None,
            primary_keys=None,
            last_modified_keys=None,
            escaped=False, execution='immediate',
            credential_id=None, polling_interval=None,
            hidden=True
        )

    @pytest.mark.civis_file_to_table
    @mock.patch('civis.io._tables._process_cleaning_results')
    @mock.patch('civis.io._tables._run_cleaning')
    def test_civis_file_to_table_table_exists(self,
                                              m_run_cleaning,
                                              m_process_cleaning_results,
                                              _m_get_api_spec):
        table = "scratch.api_client_test_fixture"
        database = 'redshift-general'
        mock_file_id = 1234
        mock_cleaned_file_id = 1235
        mock_import_id = 8675309

        self.mock_client.imports.post_files_csv.return_value\
            .id = mock_import_id
        self.mock_client.get_database_id.return_value = 42
        self.mock_client.default_credential = 713

        self.mock_client.get_table_id.return_value = 42
        m_process_cleaning_results.return_value = (
            [mock_cleaned_file_id],
            True,  # headers
            'gzip',  # compression
            'comma',  # delimiter
            None  # table_columns
        )
        m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

        with mock.patch.object(
                civis.io._tables, 'run_job',
                spec_set=True) as m_run_job:

            run_job_future = mock.MagicMock(
                spec=civis.futures.CivisFuture,
                job_id=123,
                run_id=234
            )

            m_run_job.return_value = run_job_future

            result = civis.io.civis_file_to_table(
                mock_file_id, database, table,
                existing_table_rows='truncate',
                delimiter=',',
                headers=True,
                client=self.mock_client
            )

            assert result is run_job_future
            m_run_job.assert_called_once_with(mock_import_id,
                                              client=self.mock_client,
                                              polling_interval=None)

        m_run_cleaning.assert_called_once_with(
            [mock_file_id], self.mock_client, False, True, 'comma', True
        )
        m_process_cleaning_results.assert_called_once_with(
            [mock.sentinel.cleaning_future],
            self.mock_client,
            True,
            False,
            'comma'
        )

        expected_name = 'CSV import to scratch.api_client_test_fixture'
        expected_kwargs = {
            'name': expected_name,
            'max_errors': None,
            'existing_table_rows': 'truncate',
            'column_delimiter': 'comma',
            'compression': 'gzip',
            'escaped': False,
            'execution': 'immediate',
            'loosen_types': False,
            'table_columns': None,
            'redshift_destination_options': {
                'diststyle': None, 'distkey': None,
                'sortkeys': [None, None]
            }

        }
        self.mock_client.imports.post_files_csv.assert_called_once_with(
            {'file_ids': [mock_cleaned_file_id]},
            {
                'schema': 'scratch',
                'table': 'api_client_test_fixture',
                'remote_host_id': 42,
                'credential_id': 713,
                'primary_keys': None,
                'last_modified_keys': None
            },
            True,
            **expected_kwargs
        )

    @pytest.mark.civis_file_to_table
    @mock.patch('civis.io._tables._process_cleaning_results')
    @mock.patch('civis.io._tables._run_cleaning')
    def test_civis_file_to_table_table_doesnt_exist(
            self,
            m_run_cleaning,
            m_process_cleaning_results,
            _m_get_api_spec
    ):
        table = "scratch.api_client_test_fixture"
        database = 'redshift-general'
        mock_file_id = 1234
        mock_cleaned_file_id = 1235
        mock_import_id = 8675309

        self.mock_client.imports.post_files_csv.return_value\
            .id = mock_import_id
        self.mock_client.get_database_id.return_value = 42
        self.mock_client.default_credential = 713

        self.mock_client.get_table_id.side_effect = ValueError('no table')
        mock_columns = [{'name': 'foo', 'sql_type': 'INTEGER'}]
        m_process_cleaning_results.return_value = (
            [mock_cleaned_file_id],
            True,  # headers
            'gzip',  # compression
            'comma',  # delimiter
            mock_columns  # table_columns
        )
        m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

        with mock.patch.object(
                civis.io._tables, 'run_job',
                spec_set=True) as m_run_job:

            run_job_future = mock.MagicMock(
                spec=civis.futures.CivisFuture,
                job_id=123,
                run_id=234
            )

            m_run_job.return_value = run_job_future

            result = civis.io.civis_file_to_table(
                mock_file_id, database, table,
                existing_table_rows='truncate',
                delimiter=',',
                headers=True,
                client=self.mock_client
            )

            assert result is run_job_future
            m_run_job.assert_called_once_with(mock_import_id,
                                              client=self.mock_client,
                                              polling_interval=None)

        m_run_cleaning.assert_called_once_with(
            [mock_file_id], self.mock_client, True, True, 'comma', True
        )
        m_process_cleaning_results.assert_called_once_with(
            [mock.sentinel.cleaning_future],
            self.mock_client,
            True,
            True,
            'comma'
        )

        expected_name = 'CSV import to scratch.api_client_test_fixture'
        expected_kwargs = {
            'name': expected_name,
            'max_errors': None,
            'existing_table_rows': 'truncate',
            'column_delimiter': 'comma',
            'compression': 'gzip',
            'escaped': False,
            'execution': 'immediate',
            'loosen_types': False,
            'table_columns': mock_columns,
            'redshift_destination_options': {
                'diststyle': None, 'distkey': None,
                'sortkeys': [None, None]
            }

        }
        self.mock_client.imports.post_files_csv.assert_called_once_with(
            {'file_ids': [mock_cleaned_file_id]},
            {
                'schema': 'scratch',
                'table': 'api_client_test_fixture',
                'remote_host_id': 42,
                'credential_id': 713,
                'primary_keys': None,
                'last_modified_keys': None
            },
            True,
            **expected_kwargs
        )

    @pytest.mark.civis_file_to_table
    @mock.patch('civis.io._tables._process_cleaning_results')
    @mock.patch('civis.io._tables._run_cleaning')
    def test_civis_file_to_table_multi_file(
            self,
            m_run_cleaning,
            m_process_cleaning_results,
            _m_get_api_spec
    ):
        table = "scratch.api_client_test_fixture"
        database = 'redshift-general'
        mock_file_id = [1234, 1235]
        mock_cleaned_file_ids = [1236, 1237]
        mock_import_id = 8675309

        self.mock_client.imports.post_files_csv.return_value\
            .id = mock_import_id
        self.mock_client.get_database_id.return_value = 42
        self.mock_client.default_credential = 713

        self.mock_client.get_table_id.side_effect = ValueError('no table')
        mock_columns = [{'name': 'foo', 'sql_type': 'INTEGER'}]
        m_process_cleaning_results.return_value = (
            mock_cleaned_file_ids,
            True,  # headers
            'gzip',  # compression
            'comma',  # delimiter
            mock_columns  # table_columns
        )
        m_run_cleaning.return_value = [mock.sentinel.cleaning_future1,
                                       mock.sentinel.cleaning_future2]

        with mock.patch.object(
                civis.io._tables, 'run_job',
                spec_set=True) as m_run_job:

            run_job_future = mock.MagicMock(
                spec=civis.futures.CivisFuture,
                job_id=123,
                run_id=234
            )

            m_run_job.return_value = run_job_future

            result = civis.io.civis_file_to_table(
                mock_file_id, database, table,
                existing_table_rows='truncate',
                delimiter=',',
                headers=True,
                client=self.mock_client
            )

            assert result is run_job_future
            m_run_job.assert_called_once_with(mock_import_id,
                                              client=self.mock_client,
                                              polling_interval=None)

        m_run_cleaning.assert_called_once_with(
            mock_file_id, self.mock_client, True, True, 'comma', True
        )
        m_process_cleaning_results.assert_called_once_with(
            [mock.sentinel.cleaning_future1,
             mock.sentinel.cleaning_future2],
            self.mock_client,
            True,
            True,
            'comma'
        )

        expected_name = 'CSV import to scratch.api_client_test_fixture'
        expected_kwargs = {
            'name': expected_name,
            'max_errors': None,
            'existing_table_rows': 'truncate',
            'column_delimiter': 'comma',
            'compression': 'gzip',
            'escaped': False,
            'execution': 'immediate',
            'loosen_types': True,
            'table_columns': mock_columns,
            'redshift_destination_options': {
                'diststyle': None, 'distkey': None,
                'sortkeys': [None, None]
            }

        }
        self.mock_client.imports.post_files_csv.assert_called_once_with(
            {'file_ids': mock_cleaned_file_ids},
            {
                'schema': 'scratch',
                'table': 'api_client_test_fixture',
                'remote_host_id': 42,
                'credential_id': 713,
                'primary_keys': None,
                'last_modified_keys': None
            },
            True,
            **expected_kwargs
        )

    @pytest.mark.process_cleaning_results
    def test_process_cleaning_results(self, _m_get_api_spec):
        mock_job_id = 42
        mock_run_id = 1776
        mock_file_id = 312
        fut = civis.futures.CivisFuture(poller=lambda j, r: (j, r),
                                        poller_args=(mock_job_id, mock_run_id),
                                        poll_on_creation=False)
        fut.set_result(Response({'state': 'success'}))

        self.mock_client.jobs.list_runs_outputs.return_value = [Response(
            {'object_id': mock_file_id}
        )]

        expected_columns = [{'name': 'a', 'sql_type': 'INT'},
                            {'name': 'column', 'sql_type': 'INT'}]
        expected_compression = 'gzip'
        expected_headers = True
        expected_delimiter = ','
        self.mock_client.files.get.return_value.detected_info = {
            'tableColumns': expected_columns,
            'compression': expected_compression,
            'includeHeader': expected_headers,
            'columnDelimiter': expected_delimiter
        }

        assert civis.io._tables._process_cleaning_results(
            [fut], self.mock_client, None, True, None
        ) == (
            [mock_file_id], expected_headers, expected_compression,
            expected_delimiter, expected_columns
        )

    @pytest.mark.process_cleaning_results
    def test_process_cleaning_results_raises_imports(self, _m_get_api_spec):
        mock_job_id = 42
        mock_run_id = 1776
        mock_file_id = 312
        fut = civis.futures.CivisFuture(poller=lambda j, r: (j, r),
                                        poller_args=(mock_job_id, mock_run_id),
                                        poll_on_creation=False)
        fut.set_result(Response({'state': 'success'}))

        fut2 = civis.futures.CivisFuture(
            poller=lambda j, r: (j, r),
            poller_args=(mock_job_id, mock_run_id),
            poll_on_creation=False
        )
        fut2.set_result(Response({'state': 'success'}))

        self.mock_client.jobs.list_runs_outputs.return_value = [Response(
            {'object_id': mock_file_id}
        )]

        expected_compression = 'gzip'
        expected_headers = True
        expected_cols = [{'name': 'a', 'sql_type': 'INT'},
                         {'name': 'column', 'sql_type': 'INT'}]
        resp1 = Response({
            'detected_info':
                {
                    'tableColumns': expected_cols,
                    'compression': expected_compression,
                    'includeHeader': expected_headers,
                    'columnDelimiter': ','
                }
        })

        resp2 = Response({
            'detected_info':
                {
                    'tableColumns': expected_cols,
                    'compression': expected_compression,
                    'includeHeader': expected_headers,
                    'columnDelimiter': '|'
                }
        })
        self.mock_client.files.get.side_effect = [resp1, resp2]

        with pytest.raises(CivisImportError,
                           match='Provided delimiter "|" does not match '
                                 'detected delimiter'):
            civis.io._tables._process_cleaning_results(
                [fut, fut2], self.mock_client, None, True, None
            )

    @pytest.mark.run_cleaning
    @pytest.mark.parametrize('fids', ([42], [42, 43]))
    @mock.patch('civis.io._tables.run_job',
                return_value=mock.MagicMock(spec=civis.futures.CivisFuture))
    def test_run_cleaning(self, m_run_job, fids):

        def mock_preprocess(fid):
            resp = Response({'id': fid})
            return resp

        self.mock_client.files.post_preprocess_csv\
            .side_effect = mock_preprocess

        res = civis.io._tables._run_cleaning(fids, self.mock_client, True,
                                             True, 'comma', True)

        # We should have one cleaning job per provided file id
        fid_count = len(fids)
        assert len(res) == fid_count
        self.mock_client.files.post_preprocess_csv.assert_has_calls((
            mock.call(
                file_id=fid,
                in_place=False,
                detect_table_columns=True,
                force_character_set_conversion=True,
                include_header=True,
                column_delimiter='comma',
                hidden=True)
            for fid in fids
        ))
        m_run_job.assert_has_calls((
            mock.call(
                jid, client=self.mock_client, polling_interval=None
            ) for jid in fids)
        )

    @pytest.mark.check_all_detected_info
    def test_check_all_detected_info_matching(self, _m_get_api_spec):
        headers = True
        compression = 'gzip'
        delimiter = 'comma'
        detected_info = {
            'includeHeader': True,
            'columnDelimiter': 'comma',
            'compression': 'gzip'
        }

        civis.io._tables._check_all_detected_info(
            detected_info, headers, delimiter, compression, 42
        )

    @pytest.mark.check_all_detected_info
    def test_check_all_detected_info_raises(self, _m_get_api_spec):
        headers = False
        compression = 'gzip'
        delimiter = 'comma'
        detected_info = {
            'includeHeader': True,
            'columnDelimiter': 'comma',
            'compression': 'gzip'
        }

        with pytest.raises(civis.base.CivisImportError):
            civis.io._tables._check_all_detected_info(
                detected_info, headers, delimiter, compression, 42
            )

    @pytest.mark.check_column_types
    def test_check_column_types_differing_numbers(self, _m_get_api_spec):
        table_cols = [{'name': 'col1', 'sql_type': 'INT'}]
        file_cols = [{'name': 'col1', 'sql_type': 'INT'},
                     {'name': 'col2', 'sql_type': 'FLOAT'}]

        with pytest.raises(civis.base.CivisImportError,
                           match='Expected 1 columns but file 42 has 2 columns'
                           ):
            civis.io._tables._check_column_types(table_cols, file_cols, 42)

    @pytest.mark.check_column_types
    def test_check_column_types_differing_types(self, _m_get_api_spec):
        table_cols = [{'name': 'col1', 'sql_type': 'INT'}]
        file_cols = [{'name': 'col1', 'sql_type': 'FLOAT'}]

        with pytest.raises(civis.base.CivisImportError,
                           match='Column 0: File base type was FLOAT, but '
                                 'expected INT'):
            civis.io._tables._check_column_types(table_cols, file_cols, 42)

    @pytest.mark.check_column_types
    def test_check_column_types_passing(self, _m_get_api_spec):
        table_cols = [{'name': 'col1', 'sql_type': 'INT'},
                      {'name': 'col2', 'sql_type': 'VARCHAR(42)'}]
        file_cols = [{'name': 'col1', 'sql_type': 'INT'},
                     {'name': 'col2', 'sql_type': 'VARCHAR(47)'}]

        civis.io._tables._check_column_types(table_cols, file_cols, 42)

    @pytest.mark.read_civis
    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_pandas(self, *mocks):
        expected = pd.DataFrame([[1, 2, 3]], columns=['a', 'b', 'c'])
        df = civis.io.read_civis('scratch.api_client_test_fixture',
                                 'redshift-general', use_pandas=True,
                                 polling_interval=POLL_INTERVAL)
        assert df.equals(expected)

    @pytest.mark.read_civis
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_no_pandas(self, *mocks):
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis('scratch.api_client_test_fixture',
                                   'redshift-general', use_pandas=False,
                                   polling_interval=POLL_INTERVAL)
        assert data == expected

    @pytest.mark.read_civis_sql
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_sql(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis_sql(sql, 'redshift-general',
                                       use_pandas=False,
                                       polling_interval=POLL_INTERVAL)
        assert data == expected

    @pytest.mark.dataframe_to_civis
    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch('civis.io._tables.file_to_civis')
    @mock.patch('civis.io._tables.civis_file_to_table')
    def test_dataframe_to_civis(self, m_civis_file_to_table, m_file_to_civis,
                                _m_get_api_spec):
        df = pd.DataFrame([[1, 2, 3], [2, 3, 4]])
        m_file_to_civis.return_value = 42
        mock_future = mock.create_autospec(civis.futures.CivisFuture,
                                           spec_set=True)
        m_civis_file_to_table.return_value = mock_future

        # use a mock to spy on the dataframe's to_csv method so we can
        # check on its calls without impeding its actual usage.
        with mock.patch.object(df, 'to_csv', wraps=df.to_csv) as m_to_csv:
            result = civis.io.dataframe_to_civis(
                df, 'redshift-general',
                'scratch.api_client_test_fixture',
                existing_table_rows='truncate',
                client=self.mock_client)
            assert result == mock_future

            # ANY here represents the path to which the dataframe was written
            # Since it's a temporary directory we don't know/care exactly what
            # it is
            m_to_csv.assert_called_once_with(mock.ANY, encoding='utf-8',
                                             index=False)
            out_path = m_to_csv.call_args.args[0]

        m_file_to_civis.assert_called_once_with(
            mock.ANY, 'api_client_test_fixture',
            client=self.mock_client
        )

        # Ensure that the file written to above is the same file as that
        # uploaded to Civis in this call
        assert m_file_to_civis.call_args.args[0] == out_path

        m_civis_file_to_table.assert_called_once_with(
            m_file_to_civis.return_value, 'redshift-general',
            'scratch.api_client_test_fixture',
            client=self.mock_client,
            max_errors=None, existing_table_rows="truncate",
            diststyle=None, distkey=None,
            sortkey1=None, sortkey2=None,
            delimiter=',',
            primary_keys=None,
            last_modified_keys=None,
            escaped=False, execution='immediate',
            headers=True, credential_id=None,
            polling_interval=None,
            hidden=True)

    @pytest.mark.civis_to_multifile_csv
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_multifile_csv(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        max_file_size = 32
        result = civis.io.civis_to_multifile_csv(
            sql, database='redshift-general', polling_interval=POLL_INTERVAL,
            max_file_size=max_file_size)
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
        for entry in result['entries']:
            assert entry['size'] <= max_file_size

    @pytest.mark.civis_to_multifile_csv
    @mock.patch('civis.io._tables.CivisFuture')
    @mock.patch('civis.io._tables.civis_to_file')
    @mock.patch('civis.io._tables._sql_script')
    def test_civis_to_multifile_passes_client(
            self, m_sql_script, m_civis_to_file, m_CivisFuture, *mocks):
        """Ensure the client kwarg is passed forward."""
        m_sql_script.return_value = (mock.MagicMock(), mock.MagicMock())
        # We need to write some JSON into the buffer to avoid errors.
        m_civis_to_file.side_effect = (
            lambda _, buf, *args, **kwargs: buf.write(b'{}')
        )
        mock_client = mock.MagicMock()

        civis.io.civis_to_multifile_csv('sql', 'db', client=mock_client)

        m_civis_to_file.assert_called_once_with(
            mock.ANY, mock.ANY, client=mock_client
        )

    @pytest.mark.transfer_table
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

    @pytest.mark.get_sql_select
    def test_get_sql_select(self, *mocks):
        x = "select * from schema.table"
        y = "select a, b, c from schema.table"
        table = "schema.table"
        assert civis.io._tables._get_sql_select(table) == x
        assert civis.io._tables._get_sql_select(table, ['a', 'b', 'c']) == y
        with pytest.raises(TypeError):
            civis.io._tables._get_sql_select(table, "column_a")

    @pytest.mark.download_file
    def test_download_file(self, *mocks):
        expected = '"1","2","3"\n'
        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, 'tempfile')
            civis.io._tables._download_file(self.export_url, fname,
                                            b'', 'none')
            with open(fname, "r") as f:
                data = f.read()
        assert data == expected


@pytest.mark.file_id_from_run_output
def test_file_id_from_run_output_exact():
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = \
        [Response({'name': 'spam', 'object_id': 2013,
                   'object_type': 'File'})]

    fid = civis.io.file_id_from_run_output('spam', 17, 13, client=m_client)
    assert fid == 2013


@pytest.mark.file_id_from_run_output
def test_file_id_from_run_output_approximate():
    # Test fuzzy name matching
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = \
        [Response({'name': 'spam.csv.gz', 'object_id': 2013,
                   'object_type': 'File'})]

    fid = civis.io.file_id_from_run_output('^spam', 17, 13, regex=True,
                                           client=m_client)
    assert fid == 2013


@pytest.mark.file_id_from_run_output
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


@pytest.mark.file_id_from_run_output
def test_file_id_from_run_output_no_file():
    # Get an IOError if we request a file which doesn't exist
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.return_value = [
        Response({'name': 'spam', 'object_id': 2013,
                  'object_type': 'File'})]

    with pytest.raises(FileNotFoundError) as err:
        civis.io.file_id_from_run_output('eggs', 17, 13, client=m_client)
    assert 'not an output' in str(err.value)


@pytest.mark.file_id_from_run_output
def test_file_id_from_run_output_no_run():
    # Get an IOError if we request a file from a run which doesn't exist
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.side_effect =\
        MockAPIError(404)  # Mock a run which doesn't exist

    with pytest.raises(IOError) as err:
        civis.io.file_id_from_run_output('name', 17, 13, client=m_client)
    assert 'could not find job/run id 17/13' in str(err.value).lower()


@pytest.mark.file_id_from_run_output
def test_file_id_from_run_output_platform_error():
    # Make sure we don't swallow real Platform errors
    m_client = mock.Mock()
    m_client.scripts.list_containers_runs_outputs.side_effect =\
        MockAPIError(500)  # Mock a platform error
    with pytest.raises(CivisAPIError):
        civis.io.file_id_from_run_output('name', 17, 13, client=m_client)


@pytest.mark.file_to_dataframe
@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_expired():
    m_client = mock.Mock()
    url = None
    m_client.files.get.return_value = Response({'name': 'spam.csv',
                                                'file_url': url})
    expected_err = 'Unable to locate file 121. If it previously ' + \
        'existed, it may have expired.'
    with pytest.raises(EmptyResultError, match=expected_err):
        civis.io.file_to_dataframe(121, client=m_client)


@pytest.mark.file_to_dataframe
@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer():
    m_client = mock.Mock()
    url = 'url'
    m_client.files.get.return_value = Response({'name': 'spam.csv',
                                                'file_url': url})
    with mock.patch.object(civis.io._files.pd, 'read_csv',
                           autospec=True) as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='infer', client=m_client)
        mock_read_csv.assert_called_once_with(url, compression='infer')


@pytest.mark.file_to_dataframe
@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer_gzip():
    m_client = mock.Mock()
    url = 'url'
    m_client.files.get.return_value = Response({'name': 'spam.csv.gz',
                                                'file_url': url})
    with mock.patch.object(civis.io._files.pd, 'read_csv',
                           autospec=True) as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='infer', client=m_client)
        mock_read_csv.assert_called_once_with(url, compression='gzip')


@pytest.mark.file_to_dataframe
@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_kwargs():
    m_client = mock.Mock()
    url = 'url'
    m_client.files.get.return_value = Response({'name': 'spam.csv',
                                                'file_url': url})
    with mock.patch.object(civis.io._files.pd, 'read_csv',
                           autospec=True) as mock_read_csv:
        civis.io.file_to_dataframe(121, compression='special', client=m_client,
                                   delimiter='|', nrows=10)
        mock_read_csv.assert_called_once_with(url, compression='special',
                                              delimiter='|', nrows=10)


@pytest.mark.file_to_json
@mock.patch.object(civis.io._files, 'civis_to_file', autospec=True)
def test_load_json(mock_c2f):
    obj = {'spam': 'eggs'}

    def _dump_json(file_id, buf, *args, **kwargs):
        buf.write(json.dumps(obj).encode())
    mock_c2f.side_effect = _dump_json
    out = civis.io.file_to_json(13, client=mock.Mock())
    assert out == obj


@pytest.mark.civis_to_file
@mock.patch.object(_files, 'requests', autospec=True)
def test_civis_to_file_local(mock_requests):
    # Test that a call to civis_to_file uses `requests` to grab the contents
    # of a URL given by the API client and writes it to a file.
    mock_civis = create_client_mock()
    mock_requests.get.return_value.iter_content.return_value =\
        (l.encode() for l in 'abcdef')
    with TemporaryDirectory() as tdir:
        fname = os.path.join(tdir, 'testfile')
        _files.civis_to_file(137, fname, client=mock_civis)
        with open(fname, 'rt') as _fin:
            assert _fin.read() == 'abcdef'
    mock_civis.files.get.assert_called_once_with(137)
    mock_requests.get.assert_called_once_with(
        mock_civis.files.get.return_value.file_url, stream=True)


@pytest.mark.civis_to_file
@mock.patch.object(_files, 'requests', autospec=True)
def test_civis_to_file_retries(mock_requests):
    mock_civis = create_client_mock()

    # Mock the request iter_content so it fails partway the first time.
    # Python 2.7 doesn't have the nonlocal keyword, so here's a little class to
    # track whether it's the first call.
    class UnreliableIterContent:
        def __init__(self):
            self.first_try = True

        def mock_iter_content(self, _):
            chunks = [l.encode() for l in 'abcdef']
            for i, chunk in enumerate(chunks):

                # Fail partway through on the first try.
                if self.first_try and i == 3:
                    self.first_try = False
                    raise requests.ConnectionError()

                yield chunk

    mock_requests.get.return_value.iter_content = \
        UnreliableIterContent().mock_iter_content

    # Add some data to the buffer to test that we seek to the right place
    # when retrying.
    buf = io.BytesIO(b'0123')
    buf.seek(4)

    _files.civis_to_file(137, buf, client=mock_civis)

    # Check that retries work and that the buffer position is reset.
    # If we didn't seek when retrying, we'd get abcabcdef.
    # If we seek'd to position 0, then we'd get abcdef.
    buf.seek(0)
    assert buf.read() == b'0123abcdef'

    mock_civis.files.get.assert_called_once_with(137)
    assert mock_requests.get.call_count == 2
    mock_requests.get.assert_called_with(
         mock_civis.files.get.return_value.file_url, stream=True)


@pytest.mark.file_to_civis
@pytest.mark.parametrize('input_filename', ['newname', None])
@mock.patch.object(_files, 'requests', autospec=True)
def test_file_to_civis(mock_requests, input_filename):
    # Test that file_to_civis posts a Civis File with the API client
    # and calls `requests.post` on the returned URL.
    mock_civis = create_client_mock()
    civis_name, expected_id = 'newname', 137
    mock_civis.files.post.return_value.id = expected_id
    with TemporaryDirectory() as tdir:
        fname = os.path.join(tdir, 'newname')
        with open(fname, 'wt') as _fout:
            _fout.write('abcdef')
        fid = _files.file_to_civis(fname, input_filename, expires_at=None,
                                   client=mock_civis)
    assert fid == expected_id
    mock_civis.files.post.assert_called_once_with(civis_name, expires_at=None)
    mock_requests.post.assert_called_once_with(
        mock_civis.files.post.return_value.upload_url, files=mock.ANY)


@pytest.mark.split_schema_tablename
@pytest.mark.parametrize("table,expected", [
    ('schema.table', ('schema', 'table')),
    ('schema."t.able"', ('schema', 't.able')),
    ('schema.table"', ('schema', 'table"')),
    ('"sch.ema"."t.able"', ('sch.ema', 't.able')),
    ('schema."tab""le."', ('schema', 'tab"le.')),
    ('table_with_no_schema', (None, 'table_with_no_schema')),
])
def test_split_schema_tablename(table, expected):
    assert civis.io._tables.split_schema_tablename(table) == expected


@pytest.mark.split_schema_tablename
def test_split_schema_tablename_raises():
    s = "table.with.too.many.periods"
    with pytest.raises(ValueError):
        civis.io._tables.split_schema_tablename(s)


@pytest.mark.export_to_civis_file
@mock.patch.object(civis.io._tables, '_sql_script', autospec=True,
                   return_value=[700, 1000])
def test_export_to_civis_file(mock_sql_script):
    expected = [{'file_id': 9844453}]

    mock_client = create_client_mock()
    response = Response({'state': 'success', 'output': expected})
    mock_client.scripts.get_sql_runs.return_value = response
    mock_client.scripts.post_sql

    sql = "SELECT 1"
    fut = civis.io.export_to_civis_file(sql, 'fake-db',
                                        polling_interval=POLL_INTERVAL,
                                        client=mock_client)
    data = fut.result()['output']
    assert data == expected
    mock_sql_script.assert_called_once_with(client=mock_client,
                                            sql=sql,
                                            database='fake-db',
                                            job_name=None,
                                            credential_id=None,
                                            csv_settings=None,
                                            hidden=True)


@pytest.mark.sql_script
def test_sql_script():
    sql = "SELECT SPECIAL SQL QUERY"
    export_job_id = 32
    database_id = 29
    credential_id = 3920
    response = Response({'id': export_job_id})

    mock_client = create_client_mock()
    mock_client.scripts.post_sql.return_value = response
    mock_client.get_database_id.return_value = database_id
    mock_client.default_credential = credential_id

    civis.io._tables._sql_script(client=mock_client,
                                 sql=sql,
                                 database='fake-db',
                                 job_name='My job',
                                 credential_id=None,
                                 hidden=False,
                                 csv_settings=None)
    mock_client.scripts.post_sql.assert_called_once_with(
        'My job',
        remote_host_id=database_id,
        credential_id=credential_id,
        sql=sql,
        hidden=False,
        csv_settings={})
    mock_client.scripts.post_sql_runs.assert_called_once_with(export_job_id)
