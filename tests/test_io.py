import tempfile
import csv
import gzip
import io
import itertools
import json
import os
import warnings
from functools import partial
from io import StringIO, BytesIO
from unittest import mock
from tempfile import TemporaryDirectory
import zipfile

import pytest
import requests
from requests import ConnectionError, ConnectTimeout

try:
    import pandas as pd

    has_pandas = True
except ImportError:
    has_pandas = False

try:
    import polars as pl

    has_polars = True
except ImportError:
    has_polars = False

import civis
from civis.io import _files
from civis._deprecation import DeprecatedKwargDefault
from civis.io._files import _retry
from civis.io._tables import _File
from civis.io._utils import maybe_get_random_name
from civis.response import Response
from civis.base import CivisAPIError, CivisImportError, EmptyResultError
from civis.tests.mocks import create_client_mock


POLL_INTERVAL = 0.00001

# https://circleci.com/docs/variables/#built-in-environment-variables
ON_CI = os.getenv("CI", "false").lower() == "true"


class MockAPIError(CivisAPIError):
    """A fake API error with only a status code"""

    def __init__(self, sc):
        self.status_code = sc


def _test_file(cols=None, headers=True, delimiter="comma", compression="gzip") -> _File:
    detected_info = {
        "tableColumns": cols,
        "includeHeader": headers,
        "columnDelimiter": delimiter,
        "compression": compression,
    }
    return _File(id=1, name="x.csv", detected_info=detected_info)


@mock.patch.object(_files, "requests", autospec=True)
def test_bytes_file_to_civis(mock_requests):
    mock_civis = create_client_mock()
    civis_name = "somename"
    mock_civis.files.post.return_value.id = 137
    mock_civis.files.post.return_value.url = "url"

    buf = BytesIO()
    buf.write(b"a,b,c\n1,2,3")
    buf.seek(0)

    result = civis.io.file_to_civis(buf, civis_name, client=mock_civis)

    assert isinstance(result, int)


@mock.patch.object(_files, "requests", autospec=True)
def test_zip_member_to_civis(*mocks):
    mock_civis = create_client_mock()
    mock_civis.files.post.return_value.id = 137
    mock_civis.files.post.return_value.url = "url"
    with TemporaryDirectory() as temp_dir:
        fname = os.path.join(temp_dir, "tempfile")
        with zipfile.ZipFile(fname, "w", zipfile.ZIP_DEFLATED) as zip_file:
            archive_name = "archive_name"
            zip_file.writestr(archive_name, "a,b,c\n1,2,3")
            zip_member = zip_file.namelist()[0]
            with zip_file.open(zip_member) as zip_member_buf:
                result = civis.io.file_to_civis(
                    zip_member_buf, zip_member, client=mock_civis
                )

    assert isinstance(result, int)


@mock.patch.object(_files, "requests", autospec=True)
def test_text_file_to_civis(*mocks):
    mock_civis = create_client_mock()
    mock_civis.files.post.return_value.id = 137
    mock_civis.files.post.return_value.url = "url"
    buf = StringIO()
    buf.write("a,b,c\n1,2,3")
    buf.seek(0)
    result = civis.io.file_to_civis(buf, "somename", client=mock_civis)

    assert isinstance(result, int)


@mock.patch.object(_files, "requests", autospec=True)
def test_large_file_to_civis(*mocks):
    mock_civis = create_client_mock()
    mock_civis.files.post_multipart.return_value.id = 137
    mock_civis.files.post_multipart.return_value.upload_urls = ["url"]
    curr_size = civis.io._files.MIN_MULTIPART_SIZE
    civis.io._files.MIN_MULTIPART_SIZE = 1
    with TemporaryDirectory() as temp_dir:
        fname = os.path.join(temp_dir, "tempfile")
        with open(fname, "w+b") as tmp:
            tmp.write(b"a,b,c\n1,2,3")
        with open(fname, "r+b") as tmp:
            result = civis.io.file_to_civis(tmp, fname, client=mock_civis)

        civis.io._files.MIN_MULTIPART_SIZE = curr_size

    assert isinstance(result, int)


@mock.patch("civis.io._tables.file_to_civis")
@mock.patch("civis.io._tables.civis_file_to_table")
def test_csv_to_civis(m_civis_file_to_table, m_file_to_civis):
    mock_civis = create_client_mock()
    mock_file_id = 42
    m_file_to_civis.return_value = mock_file_id
    mock_future = mock.create_autospec(civis.futures.CivisFuture, spec_set=True)
    m_civis_file_to_table.return_value = mock_future
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"

    fname = "a/tempfile"

    with mock.patch.object(
        civis.io._tables,
        "open",
        mock.mock_open(read_data="some,test,data"),
        create=True,
    ) as m_open:

        result = civis.io.csv_to_civis(
            fname, database, table, client=mock_civis, existing_table_rows="truncate"
        )

        m_file_to_civis.assert_called_once_with(
            m_open.return_value, "tempfile", client=mock_civis
        )

    assert result == mock_future

    m_civis_file_to_table.assert_called_once_with(
        mock_file_id,
        database,
        table,
        client=mock_civis,
        max_errors=None,
        existing_table_rows="truncate",
        diststyle=None,
        distkey=None,
        sortkey1=None,
        sortkey2=None,
        table_columns=None,
        delimiter=",",
        headers=None,
        primary_keys=None,
        last_modified_keys=None,
        escaped=False,
        execution="immediate",
        credential_id=None,
        polling_interval=None,
        hidden=True,
    )


@mock.patch.object(_files, "requests", autospec=True)
def test_civis_to_file(mock_requests):
    mock_civis = create_client_mock()
    mock_civis.files.get.return_value.id = 137
    mock_civis.files.get.return_value.upload_url = "url"
    mock_requests.get.return_value.iter_content.return_value = (
        ch.encode() for ch in "a,b,c\n1,2,3"
    )
    buf = BytesIO()
    civis.io.civis_to_file(137, buf, client=mock_civis)
    buf.seek(0)
    assert buf.read() == b"a,b,c\n1,2,3"


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_exists(m_run_cleaning, m_process_cleaning_results):
    mock_civis = create_client_mock()
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = 1234
    mock_cleaned_file_id = 1235
    mock_import_id = 8675309

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713

    mock_civis.databases.get_schemas_tables.return_value = Response({"name": "table1"})
    m_process_cleaning_results.return_value = (
        [mock_cleaned_file_id],
        True,  # headers
        "gzip",  # compression
        "comma",  # delimiter
        None,  # table_columns
    )
    m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

    with mock.patch.object(civis.io._tables, "run_job", spec_set=True) as m_run_job:

        run_job_future = mock.MagicMock(
            spec=civis.futures.CivisFuture, job_id=123, run_id=234
        )

        m_run_job.return_value = run_job_future

        result = civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="truncate",
            delimiter=",",
            headers=True,
            client=mock_civis,
        )

        assert result is run_job_future
        m_run_job.assert_called_once_with(
            mock_import_id, client=mock_civis, polling_interval=None
        )

    m_run_cleaning.assert_called_once_with(
        [mock_file_id], mock_civis, False, True, "comma", True
    )
    m_process_cleaning_results.assert_called_once_with(
        [mock.sentinel.cleaning_future], mock_civis, True, False, "comma"
    )

    expected_name = "CSV import to scratch.api_client_test_fixture"
    expected_kwargs = {
        "name": expected_name,
        "max_errors": None,
        "existing_table_rows": "truncate",
        "hidden": True,
        "column_delimiter": "comma",
        "compression": "gzip",
        "escaped": False,
        "execution": "immediate",
        "loosen_types": False,
        "table_columns": None,
        "redshift_destination_options": {
            "diststyle": None,
            "distkey": None,
            "sortkeys": [None, None],
        },
    }
    mock_civis.imports.post_files_csv.assert_called_once_with(
        {"file_ids": [mock_cleaned_file_id]},
        {
            "schema": "scratch",
            "table": "api_client_test_fixture",
            "remote_host_id": 42,
            "credential_id": 713,
            "primary_keys": None,
            "last_modified_keys": None,
        },
        True,
        **expected_kwargs,
    )


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_doesnt_exist(
    m_run_cleaning, m_process_cleaning_results
):
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = 1234
    mock_cleaned_file_id = 1235
    mock_import_id = 8675309
    mock_civis = create_client_mock()

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713

    mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
    mock_columns = [{"name": "foo", "sql_type": "INTEGER"}]
    m_process_cleaning_results.return_value = (
        [mock_cleaned_file_id],
        True,  # headers
        "gzip",  # compression
        "comma",  # delimiter
        mock_columns,  # table_columns
    )
    m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

    with mock.patch.object(civis.io._tables, "run_job", spec_set=True) as m_run_job:

        run_job_future = mock.MagicMock(
            spec=civis.futures.CivisFuture, job_id=123, run_id=234
        )

        m_run_job.return_value = run_job_future

        result = civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="truncate",
            delimiter=",",
            headers=True,
            client=mock_civis,
        )

        assert result is run_job_future
        m_run_job.assert_called_once_with(
            mock_import_id, client=mock_civis, polling_interval=None
        )

    m_run_cleaning.assert_called_once_with(
        [mock_file_id], mock_civis, True, True, "comma", True
    )
    m_process_cleaning_results.assert_called_once_with(
        [mock.sentinel.cleaning_future], mock_civis, True, True, "comma"
    )

    expected_name = "CSV import to scratch.api_client_test_fixture"
    expected_kwargs = {
        "name": expected_name,
        "max_errors": None,
        "existing_table_rows": "truncate",
        "hidden": True,
        "column_delimiter": "comma",
        "compression": "gzip",
        "escaped": False,
        "execution": "immediate",
        "loosen_types": True,
        "table_columns": mock_columns,
        "redshift_destination_options": {
            "diststyle": None,
            "distkey": None,
            "sortkeys": [None, None],
        },
    }
    mock_civis.imports.post_files_csv.assert_called_once_with(
        {"file_ids": [mock_cleaned_file_id]},
        {
            "schema": "scratch",
            "table": "api_client_test_fixture",
            "remote_host_id": 42,
            "credential_id": 713,
            "primary_keys": None,
            "last_modified_keys": None,
        },
        True,
        **expected_kwargs,
    )


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_doesnt_exist_all_sql_types_missing(
    m_run_cleaning, m_process_cleaning_results
):
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = 1234
    mock_cleaned_file_id = 1235
    mock_import_id = 8675309
    mock_civis = create_client_mock()

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713
    mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
    table_columns = [{"name": "a", "sql_type": ""}, {"name": "b", "sql_type": ""}]
    detected_columns = [
        {"name": "a", "sql_type": "INTEGER"},
        {"name": "b", "sql_type": "VARCHAR(42)"},
    ]
    m_process_cleaning_results.return_value = (
        [mock_cleaned_file_id],
        True,  # headers
        "gzip",  # compression
        "comma",  # delimiter
        detected_columns,  # table_columns
    )
    m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

    with mock.patch.object(civis.io._tables, "run_job", spec_set=True) as m_run_job:

        run_job_future = mock.MagicMock(
            spec=civis.futures.CivisFuture, job_id=123, run_id=234
        )

        m_run_job.return_value = run_job_future

        result = civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="truncate",
            delimiter=",",
            headers=True,
            client=mock_civis,
            table_columns=table_columns,
        )

        assert result is run_job_future
        m_run_job.assert_called_once_with(
            mock_import_id, client=mock_civis, polling_interval=None
        )

    m_run_cleaning.assert_called_once_with(
        [mock_file_id], mock_civis, True, True, "comma", True
    )
    m_process_cleaning_results.assert_called_once_with(
        [mock.sentinel.cleaning_future], mock_civis, True, True, "comma"
    )

    expected_name = "CSV import to scratch.api_client_test_fixture"
    expected_kwargs = {
        "name": expected_name,
        "max_errors": None,
        "existing_table_rows": "truncate",
        "hidden": True,
        "column_delimiter": "comma",
        "compression": "gzip",
        "escaped": False,
        "execution": "immediate",
        "loosen_types": True,
        "table_columns": detected_columns,
        "redshift_destination_options": {
            "diststyle": None,
            "distkey": None,
            "sortkeys": [None, None],
        },
    }
    mock_civis.imports.post_files_csv.assert_called_once_with(
        {"file_ids": [mock_cleaned_file_id]},
        {
            "schema": "scratch",
            "table": "api_client_test_fixture",
            "remote_host_id": 42,
            "credential_id": 713,
            "primary_keys": None,
            "last_modified_keys": None,
        },
        True,
        **expected_kwargs,
    )


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_does_not_exist_some_sql_types_missing(
    m_run_cleaning, m_process_cleaning_results
):
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = 1234
    mock_import_id = 8675309
    mock_civis = create_client_mock()

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713
    mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
    table_columns = [{"name": "a", "sql_type": "INT"}, {"name": "b", "sql_type": ""}]

    with pytest.raises(ValueError):
        civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="truncate",
            delimiter=",",
            headers=True,
            client=mock_civis,
            table_columns=table_columns,
        )


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_columns_keys_misspelled(
    m_run_cleaning, m_process_cleaning_results
):
    # Check for an error message if the `table_columns` input
    # contains keys other than the accepted ones.
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = 1234
    mock_import_id = 8675309
    mock_civis = create_client_mock()

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713
    mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
    table_columns = [{"name": "a", "sqlType": "INT"}, {"name": "b", "bad_type": ""}]

    with pytest.raises(ValueError) as err:
        civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="drop",
            delimiter=",",
            headers=True,
            client=mock_civis,
            table_columns=table_columns,
        )
    assert "must be one of ('name', 'sql_type')" in str(err.value)
    assert "also has ('bad_type', 'sqlType')" in str(err.value)


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_table_doesnt_exist_provide_table_columns(
    m_run_cleaning, m_process_cleaning_results
):

    # pytest.parametrize apparently doesn't work with unittest.TestCase
    # objects, which this ultimately inherits from, so we'll define a
    # function here and run subtests below.
    # See https://docs.pytest.org/en/stable/unittest.html.
    def run_subtest(mock_file_ids):
        table = "scratch.api_client_test_fixture"
        database = "redshift-general"
        mock_cleaned_file_ids = mock.Mock()
        mock_import_id = 8675309
        mock_civis = create_client_mock()

        mock_civis.imports.post_files_csv.return_value.id = mock_import_id
        mock_civis.get_database_id.return_value = 42
        mock_civis.default_database_credential_id = 713
        mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
        table_columns = [
            {"name": "foo", "sql_type": "INTEGER"},
            {"name": "bar", "sql_type": "VARCHAR(42)"},
        ]
        m_process_cleaning_results.return_value = (
            mock_cleaned_file_ids,
            True,  # headers
            "gzip",  # compression
            "comma",  # delimiter
            None,  # table_columns
        )
        m_run_cleaning.return_value = [mock.sentinel.cleaning_future]

        with mock.patch.object(civis.io._tables, "run_job", spec_set=True) as m_run_job:

            run_job_future = mock.MagicMock(
                spec=civis.futures.CivisFuture, job_id=123, run_id=234
            )

            m_run_job.return_value = run_job_future

            result = civis.io.civis_file_to_table(
                mock_file_ids,
                database,
                table,
                existing_table_rows="truncate",
                table_columns=table_columns,
                delimiter=",",
                headers=True,
                client=mock_civis,
            )

            assert result is run_job_future
            m_run_job.assert_called_once_with(
                mock_import_id, client=mock_civis, polling_interval=None
            )

        m_run_cleaning.assert_called_once_with(
            [mock_file_ids] if isinstance(mock_file_ids, int) else mock_file_ids,
            mock_civis,
            False,
            True,
            "comma",
            True,
        )
        m_process_cleaning_results.assert_called_once_with(
            [mock.sentinel.cleaning_future], mock_civis, True, False, "comma"
        )

        expected_name = "CSV import to scratch.api_client_test_fixture"
        expected_kwargs = {
            "name": expected_name,
            "max_errors": None,
            "existing_table_rows": "truncate",
            "hidden": True,
            "column_delimiter": "comma",
            "compression": "gzip",
            "escaped": False,
            "execution": "immediate",
            "loosen_types": False,
            "table_columns": table_columns,
            "redshift_destination_options": {
                "diststyle": None,
                "distkey": None,
                "sortkeys": [None, None],
            },
        }
        mock_civis.imports.post_files_csv.assert_called_once_with(
            {"file_ids": mock_cleaned_file_ids},
            {
                "schema": "scratch",
                "table": "api_client_test_fixture",
                "remote_host_id": 42,
                "credential_id": 713,
                "primary_keys": None,
                "last_modified_keys": None,
            },
            True,
            **expected_kwargs,
        )

    # Check that things work with a single file ID or multiple IDs.
    # In particular, we want to ensure that loosen_types is set to False
    # in both situations.
    for mock_file_ids in (1234, [1234], [1234, 1235]):
        m_run_cleaning.reset_mock()
        m_process_cleaning_results.reset_mock()
        run_subtest(mock_file_ids)


@mock.patch("civis.io._tables._process_cleaning_results")
@mock.patch("civis.io._tables._run_cleaning")
def test_civis_file_to_table_multi_file(m_run_cleaning, m_process_cleaning_results):
    table = "scratch.api_client_test_fixture"
    database = "redshift-general"
    mock_file_id = [1234, 1235]
    mock_cleaned_file_ids = [1236, 1237]
    mock_import_id = 8675309
    mock_civis = create_client_mock()

    mock_civis.imports.post_files_csv.return_value.id = mock_import_id
    mock_civis.get_database_id.return_value = 42
    mock_civis.default_database_credential_id = 713

    mock_civis.databases.get_schemas_tables.side_effect = MockAPIError(404)
    mock_columns = [{"name": "foo", "sql_type": "INTEGER"}]
    m_process_cleaning_results.return_value = (
        mock_cleaned_file_ids,
        True,  # headers
        "gzip",  # compression
        "comma",  # delimiter
        mock_columns,  # table_columns
    )
    m_run_cleaning.return_value = [
        mock.sentinel.cleaning_future1,
        mock.sentinel.cleaning_future2,
    ]

    with mock.patch.object(civis.io._tables, "run_job", spec_set=True) as m_run_job:

        run_job_future = mock.MagicMock(
            spec=civis.futures.CivisFuture, job_id=123, run_id=234
        )

        m_run_job.return_value = run_job_future

        result = civis.io.civis_file_to_table(
            mock_file_id,
            database,
            table,
            existing_table_rows="truncate",
            delimiter=",",
            headers=True,
            client=mock_civis,
        )

        assert result is run_job_future
        m_run_job.assert_called_once_with(
            mock_import_id, client=mock_civis, polling_interval=None
        )

    m_run_cleaning.assert_called_once_with(
        mock_file_id, mock_civis, True, True, "comma", True
    )
    m_process_cleaning_results.assert_called_once_with(
        [mock.sentinel.cleaning_future1, mock.sentinel.cleaning_future2],
        mock_civis,
        True,
        True,
        "comma",
    )

    expected_name = "CSV import to scratch.api_client_test_fixture"
    expected_kwargs = {
        "name": expected_name,
        "max_errors": None,
        "existing_table_rows": "truncate",
        "hidden": True,
        "column_delimiter": "comma",
        "compression": "gzip",
        "escaped": False,
        "execution": "immediate",
        "loosen_types": True,
        "table_columns": mock_columns,
        "redshift_destination_options": {
            "diststyle": None,
            "distkey": None,
            "sortkeys": [None, None],
        },
    }
    mock_civis.imports.post_files_csv.assert_called_once_with(
        {"file_ids": mock_cleaned_file_ids},
        {
            "schema": "scratch",
            "table": "api_client_test_fixture",
            "remote_host_id": 42,
            "credential_id": 713,
            "primary_keys": None,
            "last_modified_keys": None,
        },
        True,
        **expected_kwargs,
    )


def test_process_cleaning_results():
    mock_job_id = 42
    mock_run_id = 1776
    mock_file_id = 312
    mock_civis = create_client_mock()
    fut = civis.futures.CivisFuture(
        poller=lambda j, r: (j, r),
        poller_args=(mock_job_id, mock_run_id),
        poll_on_creation=False,
        client=mock_civis,
    )
    fut.set_result(Response({"state": "success"}))

    mock_civis.jobs.list_runs_outputs.return_value = [
        Response({"object_id": mock_file_id})
    ]

    expected_columns = [
        {"name": "a", "sql_type": "INT"},
        {"name": "column", "sql_type": "INT"},
    ]
    expected_compression = "gzip"
    expected_headers = True
    expected_delimiter = ","
    mock_civis.files.get.return_value = Response(
        {
            "id": mock_file_id,
            "detected_info": {
                "tableColumns": expected_columns,
                "compression": expected_compression,
                "includeHeader": expected_headers,
                "columnDelimiter": ",",
            },
            "name": "file1.csv.gz",
        }
    )

    assert civis.io._tables._process_cleaning_results(
        [fut], mock_civis, None, True, None
    ) == (
        [mock_file_id],
        expected_headers,
        expected_compression,
        expected_delimiter,
        expected_columns,
    )


def test_process_cleaning_results_raises_imports():
    mock_job_id = 42
    mock_run_id = 1776
    mock_file_id = 312
    mock_civis = create_client_mock()
    fut = civis.futures.CivisFuture(
        poller=lambda j, r: (j, r),
        poller_args=(mock_job_id, mock_run_id),
        poll_on_creation=False,
        client=mock_civis,
    )
    fut.set_result(Response({"state": "success"}))

    fut2 = civis.futures.CivisFuture(
        poller=lambda j, r: (j, r),
        poller_args=(mock_job_id, mock_run_id),
        poll_on_creation=False,
        client=mock_civis,
    )
    fut2.set_result(Response({"state": "success"}))

    mock_civis.jobs.list_runs_outputs.return_value = [
        Response({"object_id": mock_file_id})
    ]

    expected_compression = "gzip"
    expected_headers = True
    expected_cols = [
        {"name": "a", "sql_type": "INT"},
        {"name": "column", "sql_type": "INT"},
    ]
    resp1 = Response(
        {
            "id": 123,
            "detected_info": {
                "tableColumns": expected_cols,
                "compression": expected_compression,
                "includeHeader": expected_headers,
                "columnDelimiter": ",",
            },
            "name": "file1.csv.gz",
        }
    )

    resp2 = Response(
        {
            "id": 456,
            "detected_info": {
                "tableColumns": expected_cols,
                "compression": expected_compression,
                "includeHeader": expected_headers,
                "columnDelimiter": "|",
            },
            "name": "file2.csv.gz",
        }
    )
    mock_civis.files.get.side_effect = [resp1, resp2]

    regex = (
        r"All detected values for 'columnDelimiter' "
        r"must be the same, however --\n"
        r"\t, from: file 123 \(file1.csv.gz\)\n"
        r"\t| from: file 456 \(file2.csv.gz\)"
    )
    with pytest.raises(CivisImportError, match=regex):
        civis.io._tables._process_cleaning_results(
            [fut, fut2], mock_civis, None, True, None
        )


@pytest.mark.parametrize("fids", ([42], [42, 43]))
@mock.patch("civis.io._tables.run_job")
def test_run_cleaning(m_run_job, fids):

    def mock_preprocess(
        file_id,
        in_place,
        detect_table_columns=True,
        force_character_set_conversion=True,
        include_header=True,
        column_delimiter="comma",
        hidden=True,
    ):
        resp = Response({"id": file_id})
        return resp

    mock_civis = create_client_mock()

    mock_civis.files.post_preprocess_csv.side_effect = mock_preprocess
    mock_future = mock.create_autospec(civis.futures.CivisFuture, spec_set=True)
    m_run_job.return_value = mock_future
    res = civis.io._tables._run_cleaning(fids, mock_civis, True, True, "comma", True)

    # We should have one cleaning job per provided file id
    fid_count = len(fids)
    assert len(res) == fid_count
    mock_civis.files.post_preprocess_csv.assert_has_calls(
        (
            mock.call(
                file_id=fid,
                in_place=False,
                detect_table_columns=True,
                force_character_set_conversion=True,
                include_header=True,
                column_delimiter="comma",
                hidden=True,
            )
            for fid in fids
        )
    )
    m_run_job.assert_has_calls(
        (mock.call(jid, client=mock_civis, polling_interval=None) for jid in fids)
    )


def test_check_detected_info_matching():
    files = [
        _test_file(headers=True, delimiter="comma", compression="gzip"),
        _test_file(headers=True, delimiter="comma", compression="gzip"),
    ]
    for attr in ("includeHeader", "columnDelimiter", "compression"):
        civis.io._tables._check_detected_info(files, attr)


def test_check_detected_info_raises():
    files = [
        _test_file(headers=True, delimiter="comma", compression="gzip"),
        _test_file(headers=False, delimiter="pipe", compression="none"),
    ]
    for attr in ("includeHeader", "columnDelimiter", "compression"):
        with pytest.raises(civis.base.CivisImportError):
            civis.io._tables._check_detected_info(files, attr)


def test_check_column_types_differing_numbers():
    files = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file(
            [{"name": "col1", "sql_type": "INT"}, {"name": "col2", "sql_type": "FLOAT"}]
        ),
    ]
    regex = (
        r"All files must have the same number of columns, however --\n"
        r"\t1 from: file 1 \(x.csv\)\n"
        r"\t2 from: file 1 \(x.csv\)"
    )
    with pytest.raises(civis.base.CivisImportError, match=regex):
        civis.io._tables._check_column_types(files)


def test_check_column_types_differing_types():
    files = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file([{"name": "col1", "sql_type": "FLOAT"}]),
    ]
    regex = (
        r"All sql_types for column 'col1' must be the same, however --\n"
        r"\tINT from: file 1 \(x.csv\)\n"
        r"\tFLOAT from: file 1 \(x.csv\)"
    )
    with pytest.raises(civis.base.CivisImportError, match=regex):
        civis.io._tables._check_column_types(files)


def test_check_column_types_passing():
    files = [
        _test_file(
            [
                {"name": "col1", "sql_type": "INT"},
                {"name": "col2", "sql_type": "VARCHAR(42)"},
            ]
        ),
        _test_file(
            [
                {"name": "col1", "sql_type": "INT"},
                {"name": "col2", "sql_type": "VARCHAR(47)"},
            ]
        ),
    ]
    actual, allow_inconsistent_headers = civis.io._tables._check_column_types(files)
    expected = [
        {"name": "col1", "sql_type": "INT"},
        {"name": "col2", "sql_type": "VARCHAR(42)"},
    ]
    assert actual == expected
    assert allow_inconsistent_headers is False


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
@mock.patch("civis.io._tables.file_to_civis")
@mock.patch("civis.io._tables.civis_file_to_table")
def test_dataframe_to_civis_pandas(m_civis_file_to_table, m_file_to_civis):
    mock_civis = create_client_mock()
    df = pd.DataFrame([[1, 2, 3], [2, 3, 4]])
    m_file_to_civis.return_value = 42
    mock_future = mock.create_autospec(civis.futures.CivisFuture, spec_set=True)
    m_civis_file_to_table.return_value = mock_future

    # use a mock to spy on the dataframe's to_csv method so we can
    # check on its calls without impeding its actual usage.
    with mock.patch.object(df, "to_csv", wraps=df.to_csv) as m_to_csv:
        result = civis.io.dataframe_to_civis(
            df,
            "redshift-general",
            "scratch.api_client_test_fixture",
            existing_table_rows="truncate",
            client=mock_civis,
        )
        assert result == mock_future

        # ANY here represents the path to which the dataframe was written
        # Since it's a temporary directory we don't know/care exactly what
        # it is
        m_to_csv.assert_called_once_with(mock.ANY, encoding="utf-8", index=False)
        out_path = m_to_csv.call_args.args[0]

    m_file_to_civis.assert_called_once_with(
        mock.ANY, "api_client_test_fixture", client=mock_civis
    )

    # Ensure that the file written to above is the same file as that
    # uploaded to Civis in this call
    assert m_file_to_civis.call_args.args[0] == out_path

    m_civis_file_to_table.assert_called_once_with(
        m_file_to_civis.return_value,
        "redshift-general",
        "scratch.api_client_test_fixture",
        client=mock_civis,
        max_errors=None,
        existing_table_rows="truncate",
        diststyle=None,
        distkey=None,
        sortkey1=None,
        sortkey2=None,
        table_columns=None,
        delimiter=",",
        primary_keys=None,
        last_modified_keys=None,
        escaped=False,
        execution="immediate",
        headers=True,
        credential_id=None,
        polling_interval=None,
        hidden=True,
    )


@pytest.mark.skipif(not ON_CI and not has_polars, reason="polars not installed")
@mock.patch("civis.io._tables.file_to_civis")
@mock.patch("civis.io._tables.civis_file_to_table")
def test_dataframe_to_civis_polars(m_civis_file_to_table, m_file_to_civis):
    mock_civis = create_client_mock()
    df = pl.DataFrame([[1, 2, 3], [2, 3, 4]], orient="row")
    m_file_to_civis.return_value = 42
    mock_future = mock.create_autospec(civis.futures.CivisFuture, spec_set=True)
    m_civis_file_to_table.return_value = mock_future

    # use a mock to spy on the dataframe's to_csv method so we can
    # check on its calls without impeding its actual usage.
    with mock.patch.object(df, "write_csv", wraps=df.write_csv) as m_write_csv:
        result = civis.io.dataframe_to_civis(
            df,
            "redshift-general",
            "scratch.api_client_test_fixture",
            existing_table_rows="truncate",
            client=mock_civis,
        )
        assert result == mock_future

        # ANY here represents the path to which the dataframe was written
        # Since it's a temporary directory we don't know/care exactly what
        # it is
        m_write_csv.assert_called_once_with(mock.ANY)
        out_path = m_write_csv.call_args.args[0]

    m_file_to_civis.assert_called_once_with(
        mock.ANY, "api_client_test_fixture", client=mock_civis
    )

    # Ensure that the file written to above is the same file as that
    # uploaded to Civis in this call
    assert m_file_to_civis.call_args.args[0] == out_path

    m_civis_file_to_table.assert_called_once_with(
        m_file_to_civis.return_value,
        "redshift-general",
        "scratch.api_client_test_fixture",
        client=mock_civis,
        max_errors=None,
        existing_table_rows="truncate",
        diststyle=None,
        distkey=None,
        sortkey1=None,
        sortkey2=None,
        table_columns=None,
        delimiter=",",
        primary_keys=None,
        last_modified_keys=None,
        escaped=False,
        execution="immediate",
        headers=True,
        credential_id=None,
        polling_interval=None,
        hidden=True,
    )


def test_check_column_types_coerce_to_varchar():
    case1 = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file([{"name": "col1", "sql_type": "VARCHAR(42)"}]),
    ]
    case2 = [
        _test_file([{"name": "col1", "sql_type": "VARCHAR(42)"}]),
        _test_file([{"name": "col1", "sql_type": "INT"}]),
    ]
    case3 = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file([{"name": "col1", "sql_type": "VARCHAR(42)"}]),
        _test_file([{"name": "col1", "sql_type": "FLOAT"}]),
    ]
    case4 = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file([{"name": "col1", "sql_type": "FLOAT"}]),
        _test_file([{"name": "col1", "sql_type": "VARCHAR(42)"}]),
    ]
    case5 = [
        _test_file([{"name": "col1", "sql_type": "INT"}]),
        _test_file([{"name": "col1", "sql_type": "VARCHAR(42)"}]),
        _test_file([{"name": "col1", "sql_type": "FLOAT"}]),
        _test_file([{"name": "col1", "sql_type": "VARCHAR(8)"}]),
    ]
    for files in (case1, case2, case3, case4, case5):
        actual, allow_inconsistent_headers = civis.io._tables._check_column_types(files)
        expected = [{"name": "col1", "sql_type": "VARCHAR"}]
        assert actual == expected, f"failed for {files}"
        assert allow_inconsistent_headers is True


@mock.patch("civis.io._tables.CivisFuture")
@mock.patch("civis.io._tables.civis_to_file")
@mock.patch("civis.io._tables._sql_script")
def test_civis_to_multifile_passes_client(
    m_sql_script, m_civis_to_file, m_CivisFuture, *mocks
):
    """Ensure the client kwarg is passed forward."""
    m_sql_script.return_value = (mock.MagicMock(), mock.MagicMock())
    # We need to write some JSON into the buffer to avoid errors.
    m_civis_to_file.side_effect = lambda _, buf, *args, **kwargs: buf.write(b"{}")
    mock_client = mock.MagicMock()

    civis.io.civis_to_multifile_csv("sql", "db", client=mock_client)

    m_civis_to_file.assert_called_once_with(mock.ANY, mock.ANY, client=mock_client)


@mock.patch("civis.io.query_civis")
@mock.patch("civis.io.transfer_table")
def test_transfer_table(m_transfer_table, m_query_civis):
    mock_civis = create_client_mock()
    mock_future = mock.create_autospec(civis.futures.CivisFuture)
    resp = Response(
        {"state": "succeeded"},
    )
    attrs = {"result.return_value": resp, "state": "succeeded"}
    mock_future.configure_mock(**attrs)
    m_transfer_table.return_value = mock_future
    result = civis.io.transfer_table(
        "redshift-general",
        "redshift-test",
        "scratch.api_client_test_fixture",
        "scratch.api_client_test_fixture",
        polling_interval=POLL_INTERVAL,
        client=mock_civis,
    )
    result = result.result()
    assert result.state == "succeeded"

    # check for side effect
    m_query_civis.return_value = mock_future
    sql = "select * from scratch.api_client_test_fixture"
    result = civis.io.query_civis(
        sql, "redshift-test", polling_interval=POLL_INTERVAL, client=mock_civis
    ).result()
    assert result.state == "succeeded"


@mock.patch("civis.io._tables._download_file")
def test_download_file(m_download_file):
    expected = '"1","2","3"\n'
    m_download_file.return_value = expected
    with TemporaryDirectory() as temp_dir:
        fname = os.path.join(temp_dir, "tempfile")
        with open(fname, "w") as tmp:
            tmp.write(expected)
        civis.io._tables._download_file("test_url", fname, b"", "none")
        with open(fname, "r") as f:
            data = f.read()
    assert data == expected


def test_get_sql_select(*mocks):
    x = "select * from schema.table"
    y = "select a, b, c from schema.table"
    table = "schema.table"
    assert civis.io._tables._get_sql_select(table) == x
    assert civis.io._tables._get_sql_select(table, ["a", "b", "c"]) == y
    with pytest.raises(TypeError):
        civis.io._tables._get_sql_select(table, "column_a")


def test_file_id_from_run_output_exact():
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.return_value = [
        Response({"name": "spam", "object_id": 2013, "object_type": "File"})
    ]

    fid = civis.io.file_id_from_run_output("spam", 17, 13, client=m_client)
    assert fid == 2013


def test_file_id_from_run_output_approximate():
    # Test fuzzy name matching
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.return_value = [
        Response({"name": "spam.csv.gz", "object_id": 2013, "object_type": "File"})
    ]

    fid = civis.io.file_id_from_run_output("^spam", 17, 13, regex=True, client=m_client)
    assert fid == 2013


def test_file_id_from_run_output_approximate_multiple():
    # Fuzzy name matching with muliple matches should return the first
    m_cl = mock.Mock()
    m_cl.jobs.list_runs_outputs.return_value = [
        Response({"name": "spam.csv.gz", "object_id": 2013, "object_type": "File"}),
        Response({"name": "eggs.csv.gz", "object_id": 2014, "object_type": "File"}),
    ]

    fid = civis.io.file_id_from_run_output(".csv", 17, 13, regex=True, client=m_cl)
    assert fid == 2013


def test_file_id_from_run_output_no_file():
    # Get an IOError if we request a file which doesn't exist
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.return_value = [
        Response({"name": "spam", "object_id": 2013, "object_type": "File"})
    ]

    with pytest.raises(FileNotFoundError) as err:
        civis.io.file_id_from_run_output("eggs", 17, 13, client=m_client)
    assert "not an output" in str(err.value)


def test_file_id_from_run_output_no_run():
    # Get an IOError if we request a file from a run which doesn't exist
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.side_effect = MockAPIError(
        404
    )  # Mock a run which doesn't exist

    with pytest.raises(IOError) as err:
        civis.io.file_id_from_run_output("name", 17, 13, client=m_client)
    assert "could not find job/run id 17/13" in str(err.value).lower()


def test_file_id_from_run_output_platform_error():
    # Make sure we don't swallow real Platform errors
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.side_effect = MockAPIError(
        500
    )  # Mock a platform error
    with pytest.raises(CivisAPIError):
        civis.io.file_id_from_run_output("name", 17, 13, client=m_client)


def test_file_id_from_run_output_no_filename():
    m_client = mock.Mock()
    m_client.jobs.list_runs_outputs.return_value = [
        Response({"name": "spam.csv.gz", "object_id": 2013, "object_type": "File"}),
        Response({"name": "eggs.csv.gz", "object_id": 2014, "object_type": "File"}),
    ]

    fid = civis.io.file_id_from_run_output(".*?", 17, 13, regex=True, client=m_client)
    assert fid == 2013


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_expired():
    m_client = mock.Mock()
    url = None
    m_client.files.get.return_value = Response({"name": "spam.csv", "file_url": url})
    expected_err = (
        "Unable to locate file 121. If it previously " + "existed, it may have expired."
    )
    with pytest.raises(EmptyResultError, match=expected_err):
        civis.io.file_to_dataframe(121, client=m_client)


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer():
    m_client = mock.Mock()
    url = "url"
    m_client.files.get.return_value = Response({"name": "spam.csv", "file_url": url})
    with mock.patch.object(
        civis.io._files.pd, "read_csv", autospec=True
    ) as mock_read_csv:
        civis.io.file_to_dataframe(121, compression="infer", client=m_client)
        mock_read_csv.assert_called_once_with(url, compression="infer")


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer_gzip():
    m_client = mock.Mock()
    url = "url"
    m_client.files.get.return_value = Response({"name": "spam.csv.gz", "file_url": url})
    with mock.patch.object(
        civis.io._files.pd, "read_csv", autospec=True
    ) as mock_read_csv:
        civis.io.file_to_dataframe(121, compression="infer", client=m_client)
        mock_read_csv.assert_called_once_with(url, compression="gzip")


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_kwargs():
    m_client = mock.Mock()
    url = "url"
    m_client.files.get.return_value = Response({"name": "spam.csv", "file_url": url})
    with mock.patch.object(
        civis.io._files.pd, "read_csv", autospec=True
    ) as mock_read_csv:
        civis.io.file_to_dataframe(
            121, compression="special", client=m_client, delimiter="|", nrows=10
        )
        mock_read_csv.assert_called_once_with(
            url, compression="special", delimiter="|", nrows=10
        )


@pytest.mark.skipif(not ON_CI and not has_polars, reason="polars not installed")
def test_file_to_dataframe_polars():
    m_client = mock.Mock()
    url = "url"
    m_client.files.get.return_value = Response({"name": "spam.csv", "file_url": url})
    with mock.patch.object(
        civis.io._files.pl, "read_csv", autospec=True
    ) as mock_read_csv:
        civis.io.file_to_dataframe(
            121, return_as="polars", client=m_client, separator="|", n_rows=10
        )
        mock_read_csv.assert_called_once_with(url, separator="|", n_rows=10)


@mock.patch.object(civis.io._files, "civis_to_file", autospec=True)
def test_load_json(mock_c2f):
    obj = {"spam": "eggs"}

    def _dump_json(file_id, buf, *args, **kwargs):
        buf.write(json.dumps(obj).encode())

    mock_c2f.side_effect = _dump_json
    out = civis.io.file_to_json(13, client=mock.Mock())
    assert out == obj


@mock.patch.object(_files, "requests", autospec=True)
def test_civis_to_file_local(mock_requests):
    # Test that a call to civis_to_file uses `requests` to grab the contents
    # of a URL given by the API client and writes it to a file.
    mock_civis = create_client_mock()
    mock_requests.get.return_value.iter_content.return_value = (
        ch.encode() for ch in "abcdef"
    )
    with TemporaryDirectory() as tdir:
        fname = os.path.join(tdir, "testfile")
        _files.civis_to_file(137, fname, client=mock_civis)
        with open(fname, "rt") as _fin:
            assert _fin.read() == "abcdef"
    mock_civis.files.get.assert_called_once_with(137)
    mock_requests.get.assert_called_once_with(
        mock_civis.files.get.return_value.file_url, stream=True, timeout=60
    )


@mock.patch.object(_files, "requests", autospec=True)
def test_civis_to_file_retries(mock_requests):
    mock_civis = create_client_mock()

    first_try = True

    # Mock the request iter_content so it fails partway the first time.
    def mock_iter_content(_):
        nonlocal first_try
        chunks = [ch.encode() for ch in "abcdef"]
        for i, chunk in enumerate(chunks):

            # Fail partway through on the first try.
            if first_try and i == 3:
                first_try = False
                raise requests.ConnectionError()

            yield chunk

    mock_requests.get.return_value.iter_content = mock_iter_content

    # Add some data to the buffer to test that we seek to the right place
    # when retrying.
    buf = io.BytesIO(b"0123")
    buf.seek(4)

    _files.civis_to_file(137, buf, client=mock_civis)

    # Check that retries work and that the buffer position is reset.
    # If we didn't seek when retrying, we'd get abcabcdef.
    # If we seek'd to position 0, then we'd get abcdef.
    buf.seek(0)
    assert buf.read() == b"0123abcdef"

    mock_civis.files.get.assert_called_once_with(137)
    assert mock_requests.get.call_count == 2
    mock_requests.get.assert_called_with(
        mock_civis.files.get.return_value.file_url, stream=True, timeout=60
    )


@pytest.mark.parametrize("input_filename", ["newname", None])
@mock.patch.object(_files, "requests", autospec=True)
def test_file_to_civis(mock_requests, input_filename):
    # Test that file_to_civis posts a Civis File with the API client
    # and calls `requests.post` on the returned URL.
    mock_civis = create_client_mock()
    civis_name, expected_id = "newname", 137
    mock_civis.files.post.return_value.id = expected_id
    with TemporaryDirectory() as tdir:
        fname = os.path.join(tdir, "newname")
        with open(fname, "wt") as _fout:
            _fout.write("abcdef")
        fid = _files.file_to_civis(
            fname, input_filename, expires_at=None, client=mock_civis
        )
    assert fid == expected_id
    mock_civis.files.post.assert_called_once_with(civis_name, expires_at=None)
    mock_requests.post.assert_called_once_with(
        mock_civis.files.post.return_value.upload_url, files=mock.ANY, timeout=60
    )


def test_file_to_civis_error_for_description_too_long():
    with TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "some_data")
        with open(file_path, "wb") as f:
            f.write(b"foobar")
        with pytest.raises(ValueError, match="longer than 512 characters"):
            _files.file_to_civis(
                file_path, client=create_client_mock(), description="a" * 513
            )


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
@pytest.mark.parametrize(
    "func, should_add_description",
    itertools.product(
        [
            partial(_files.file_to_civis, io.BytesIO(b"some_data"), name="abc"),
            partial(_files.dataframe_to_file, pd.DataFrame({"a": [1]}), name="abc"),
            partial(_files.json_to_file, {"a": 1}, name="abc"),
        ],
        (True, False),
    ),
)
@mock.patch.object(_files, "requests", autospec=True)
def test_file_description_attribute_added_or_not(
    mock_requests, func, should_add_description
):
    mock_client = create_client_mock()
    description = "some_description"
    if should_add_description:
        func(description=description, client=mock_client)
        mock_client.files.post.assert_called_with("abc", description=description)
    else:
        func(client=mock_client)
        mock_client.files.post.assert_called_with("abc")


@pytest.mark.parametrize(
    "table,expected",
    [
        ("schema.table", ("schema", "table")),
        ('schema."t.able"', ("schema", "t.able")),
        ('schema.table"', ("schema", 'table"')),
        ('"sch.ema"."t.able"', ("sch.ema", "t.able")),
        ('schema."tab""le."', ("schema", 'tab"le.')),
        ("table_with_no_schema", (None, "table_with_no_schema")),
    ],
)
def test_split_schema_tablename(table, expected):
    assert civis.io._tables.split_schema_tablename(table) == expected


def test_split_schema_tablename_raises():
    s = "table.with.too.many.periods"
    with pytest.raises(ValueError):
        civis.io._tables.split_schema_tablename(s)


@mock.patch.object(
    civis.io._tables, "_sql_script", autospec=True, return_value=[700, 1000]
)
def test_export_to_civis_file(mock_sql_script):
    expected = [{"file_id": 9844453}]

    mock_client = create_client_mock()
    response = Response({"state": "success", "output": expected})
    mock_client.scripts.get_sql_runs.return_value = response
    mock_client.scripts.post_sql

    sql = "SELECT 1"
    fut = civis.io.export_to_civis_file(
        sql, "fake-db", polling_interval=POLL_INTERVAL, client=mock_client
    )
    data = fut.result()["output"]
    assert data == expected
    mock_sql_script.assert_called_once_with(
        client=mock_client,
        sql=sql,
        database="fake-db",
        job_name=None,
        credential_id=None,
        csv_settings=None,
        hidden=True,
        sql_params_arguments=None,
    )


def test_sql_script():
    sql = "SELECT SPECIAL SQL QUERY"
    export_job_id = 32
    database_id = 29
    credential_id = 3920
    response = Response({"id": export_job_id})

    mock_client = create_client_mock()
    mock_client.scripts.post_sql.return_value = response
    mock_client.get_database_id.return_value = database_id
    mock_client.default_database_credential_id = credential_id

    civis.io._tables._sql_script(
        client=mock_client,
        sql=sql,
        database="fake-db",
        job_name="My job",
        credential_id=None,
        hidden=False,
        csv_settings=None,
    )
    mock_client.scripts.post_sql.assert_called_once_with(
        "My job",
        remote_host_id=database_id,
        credential_id=credential_id,
        sql=sql,
        hidden=False,
        csv_settings={},
    )
    mock_client.scripts.post_sql_runs.assert_called_once_with(export_job_id)


@mock.patch.object(civis.io._tables, "requests")
def test_read_civis_sql_no_dataframe_special_encoding_sad_path(m_requests):
    # Set up a mock client object for what civis.io.read_civis_sql needs.
    m_client = create_client_mock()
    m_client.scripts.get_sql_runs.return_value = Response(
        {
            "output": [{"path": "blah", "file_id": 123, "output_name": "blah"}],
            "state": "success",
        }
    )
    m_response = mock.Mock()
    m_response.raw.read.return_value = gzip.compress(
        # Intentionally using non-ASCII chars like è to mess with encoding
        "foo,bar\n123,très bien\n".encode("latin-1")
    )
    m_requests.get.return_value = m_response

    # The data was encoded in latin-1, but we don't specify this at
    # the `encoding` param in the read_civis_sql call,
    # which should raise UnicodeDecodeError.
    with pytest.raises(UnicodeDecodeError):
        civis.io.read_civis_sql(
            "select 1",
            "db",
            return_as="list",
            client=m_client,
            polling_interval=POLL_INTERVAL,
        )


@mock.patch.object(civis.io._tables, "requests")
def test_read_civis_sql_no_dataframe_special_encoding_happy_path(m_requests):
    # Set up a mock client object for what civis.io.read_civis_sql needs.
    m_client = create_client_mock()
    m_client.scripts.get_sql_runs.return_value = Response(
        {
            "output": [{"path": "blah", "file_id": 123, "output_name": "blah"}],
            "state": "success",
        }
    )

    # Intentionally using non-ASCII chars like è to mess with encoding
    expected_data = "foo,bar\n123,très bien\n"
    encoding = "latin-1"

    m_response = mock.Mock()

    # The helper function _decompress_stream() calls
    # response.raw.read(CHUNK_SIZE) in a while loop.
    # In the mock `m_response.raw.read` here, we use side_effect so that
    # we get the data back from the first call and no data in the second call
    # inside the while loop (because the data has all been consumed in the
    # first call).
    m_response.raw.read.side_effect = [
        gzip.compress(expected_data.encode(encoding)),
        b"",
    ]
    m_requests.get.return_value = m_response

    actual_data = civis.io.read_civis_sql(
        "select 1",
        "db",
        return_as="list",
        client=m_client,
        polling_interval=POLL_INTERVAL,
        encoding=encoding,
    )
    assert list(csv.reader(io.StringIO(expected_data))) == actual_data


@mock.patch.object(civis.io._tables, "requests")
def test_read_civis_sql_no_dataframe(m_requests):
    # Set up a mock client object for what civis.io.read_civis_sql needs.
    m_client = create_client_mock()
    m_client.scripts.get_sql_runs.return_value = Response(
        {
            "output": [{"path": "blah", "file_id": 123, "output_name": "blah"}],
            "state": "success",
        }
    )

    expected_data = "foo,bar\n123,very good\n"

    m_response = mock.Mock()
    # The helper function _decompress_stream() calls
    # response.raw.read(CHUNK_SIZE) in a while loop.
    # In the mock `m_response.raw.read` here, we use side_effect so that
    # we get the data back from the first call and no data in the second call
    # inside the while loop (because the data has all been consumed in the
    # first call).
    m_response.raw.read.side_effect = [
        gzip.compress(expected_data.encode("utf-8")),
        b"",
    ]
    m_requests.get.return_value = m_response

    actual_data = civis.io.read_civis_sql(
        "select 1",
        "db",
        return_as="list",
        client=m_client,
        polling_interval=POLL_INTERVAL,
    )
    assert list(csv.reader(io.StringIO(expected_data))) == actual_data


@pytest.mark.skipif(not ON_CI and not has_pandas, reason="pandas not installed")
def test_read_civis_sql_pandas():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_csv_path = os.path.join(tmp_dir, "data.csv")
        expected_data = "foo,bar\n123,very good\n"
        with open(tmp_csv_path, "wb") as f:
            f.write(gzip.compress(expected_data.encode("utf-8")))

        # Set up a mock client object for what civis.io.read_civis_sql needs.
        m_client = create_client_mock()
        m_client.scripts.get_sql_runs.return_value = Response(
            {
                "output": [
                    {"path": tmp_csv_path, "file_id": 123, "output_name": "blah"}
                ],
                "state": "success",
            }
        )
        actual_data = civis.io.read_civis_sql(
            "select 1",
            "db",
            return_as="pandas",
            client=m_client,
            polling_interval=POLL_INTERVAL,
        )
        assert pd.read_csv(io.StringIO(expected_data)).equals(actual_data)


@pytest.mark.skipif(not ON_CI and not has_polars, reason="polars not installed")
def test_read_civis_sql_polars():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_csv_path = os.path.join(tmp_dir, "data.csv")
        expected_data = "foo,bar\n123,very good\n"
        with open(tmp_csv_path, "wb") as f:
            f.write(gzip.compress(expected_data.encode("utf-8")))

        # Set up a mock client object for what civis.io.read_civis_sql needs.
        m_client = create_client_mock()
        m_client.scripts.get_sql_runs.return_value = Response(
            {
                "output": [
                    {"path": tmp_csv_path, "file_id": 123, "output_name": "blah"}
                ],
                "state": "success",
            }
        )
        actual_data = civis.io.read_civis_sql(
            "select 1",
            "db",
            return_as="polars",
            client=m_client,
            polling_interval=POLL_INTERVAL,
        )
        assert pl.read_csv(io.StringIO(expected_data)).equals(actual_data)


def test_io_no_retry():
    @_retry(ConnectionError, retries=4, delay=0.1)
    def succeeds():
        counter["i"] += 1
        return "success"

    counter = dict(i=0)
    test_result = succeeds()

    assert test_result == "success"
    assert counter["i"] == 1


def test_io_retry_once():
    @_retry(ConnectionError, retries=4, delay=0.1)
    def fails_once():
        counter["i"] += 1
        if counter["i"] < 2:
            raise ConnectionError("failed")
        else:
            return "success"

    counter = dict(i=0)
    test_result = fails_once()

    assert test_result == "success"
    assert counter["i"] == 2


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_io_retry_limit_reached(m_sleep):
    @_retry(ConnectionError, retries=4, delay=0.1)
    def always_fails():
        counter["i"] += 1
        raise ConnectionError("failed")

    counter = dict(i=0)
    pytest.raises(ConnectionError, always_fails)
    assert counter["i"] == 5


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_io_retry_multiple_exceptions(m_sleep):
    @_retry((ConnectionError, ConnectTimeout), retries=4, delay=0.1)
    def raise_multiple_exceptions():
        counter["i"] += 1
        if counter["i"] == 1:
            raise ConnectionError("one error")
        elif counter["i"] == 2:
            raise requests.ConnectTimeout("another error")
        else:
            return "success"

    counter = dict(i=0)
    test_result = raise_multiple_exceptions()

    assert test_result == "success"
    assert counter["i"] == 3


def test_io_retry_unexpected_exception():
    @_retry(ConnectionError, retries=4, delay=0.1)
    def raise_unexpected_error():
        raise ValueError("unexpected error")

    pytest.raises(ValueError, raise_unexpected_error)


@mock.patch("civis.io._utils.uuid")
def test_maybe_random_name_random(mock_uuid):
    random_name = "11111"
    mock_uuid.uuid4.return_value = mock.Mock(hex=random_name)
    assert maybe_get_random_name(None) == random_name


def test_maybe_random_name_not_random():
    given_name = "22222"
    assert maybe_get_random_name(given_name) == given_name


@pytest.mark.skipif(
    not ON_CI and not (has_pandas and has_polars),
    reason="pandas or polars not installed",
)
@pytest.mark.parametrize(
    "func_name, use_pandas, return_as",
    itertools.product(
        ("read_civis_sql", "read_civis"),
        (True, False, DeprecatedKwargDefault()),
        ("list", "pandas", "polars"),
    ),
)
@mock.patch.object(civis.io._tables, "requests")
def test_warns_or_raise_exception_for_deprecated_use_pandas(
    m_requests, func_name, use_pandas, return_as
):
    if isinstance(use_pandas, DeprecatedKwargDefault):
        warn_or_raise = None
    elif use_pandas is True and return_as == "polars":
        warn_or_raise = ValueError
    elif use_pandas is False and return_as == "pandas":
        warn_or_raise = ValueError
    else:
        warn_or_raise = FutureWarning

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_csv_path = os.path.join(tmp_dir, "data.csv")
        expected_data = "foo,bar\n123,very good\n"
        with open(tmp_csv_path, "wb") as f:
            f.write(gzip.compress(expected_data.encode("utf-8")))

        # Set up a mock client object for what civis.io.read_civis(_sql) needs.
        m_client = create_client_mock()
        m_client.scripts.get_sql_runs.return_value = Response(
            {
                "output": [
                    {"path": tmp_csv_path, "file_id": 123, "output_name": "blah"}
                ],
                "state": "success",
            }
        )
        m_response = mock.Mock()
        # The helper function _decompress_stream() calls
        # response.raw.read(CHUNK_SIZE) in a while loop.
        # In the mock `m_response.raw.read` here, we use side_effect so that
        # we get the data back from the first call and no data in the second call
        # inside the while loop (because the data has all been consumed in the
        # first call).
        m_response.raw.read.side_effect = [
            gzip.compress(expected_data.encode("utf-8")),
            b"",
        ]
        m_requests.get.return_value = m_response

        func = getattr(civis.io, func_name)
        shared_args = dict(
            database="db",
            use_pandas=use_pandas,
            return_as=return_as,
            client=m_client,
            polling_interval=POLL_INTERVAL,
        )
        if func_name == "read_civis":
            args = {"table": "schema.tablename", **shared_args}
        else:
            args = {"sql": "select * from schema.tablename", **shared_args}

        if warn_or_raise and warn_or_raise.__base__.__name__ == "Warning":
            with pytest.warns(warn_or_raise):
                func(**args)
        elif warn_or_raise and warn_or_raise.__base__.__name__ == "Exception":
            with pytest.raises(warn_or_raise):
                func(**args)
        else:
            # Check that no warning is emitted.
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                func(**args)
