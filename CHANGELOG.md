# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased

### Added
- ``civis.io.dataframe_to_civis``, ``civis.io.csv_to_civis``, and ``civis.io.civis_file_to_table`` functions now support the `diststyle` parameter.
- New notebook-related CLI commands: "new", "up", "down", and "open".
- Additional documentation for using the Civis joblib backend (#199)
- Documented additional soft dependencies for CivisML (#203)

### Changed
- Changed `ModelPipeline.train` default for `n_jobs` from 4 to `None`,
  so that `n_jobs` will be dynamically calculated by default (#203)
- Use "feather"-formatted files to send data from users to CivisML, if possible.
  Require this when using ``pd.Categorical`` types, since CSVs require us to
  re-infer column types, and this can fail. Using feather should also give a
  speed improvement; it reads and writes faster than CSVs and produces smaller files (#200).

### Fixed
- Restored the pre-v1.7.0 default behavior of the ``joblib`` backend by setting the ``remote_backend``
  parameter default to 'sequential' as opposed to 'civis'. The default of 'civis' would launch additional
  containers in nested calls to ``joblib.Parallel``. (#205)

### Performance Enhancements
- ``civis.io.file_to_civis`` now uses additional file handles for multipart upload instead of writing to disk to reduce disk usage
- ``civis.io.dataframe_to_civis`` writes dataframes to disk instead of using an in memory buffer

## 1.7.2 - 2018-01-09
### Fixed
- Relaxed requirement on ``cloudpickle`` version number (#187)
- Restore previous behavior of ``civis.io.civis_to_csv`` when using "compression='gzip'" (#195)

## 1.7.1 - 2017-11-16
### Fixed
- Specify escape character in ``civis.io.read_civis_sql`` when performing parallel unload
- Issue uploading files in ``civis.io.file_to_civis``
- Revert performance enhancement that will change format of file produced by ``civis.io.civis_to_csv``

## 1.7.0 - 2017-11-15
### Changed
- Updated CivisML template ids to v2.0 (#139)
- Optional arguments to API endpoints now display in function signatures.
  Function signatures show a default value of "DEFAULT"; arguments will still
  only be transmitted to the Civis Platform API when explicitly provided. (#140)
- ``APIClient.feature_flags`` has been deprecated to avoid a name collision
   with the feature_flags endpoint. In v2.0.0, ``APIClient.featureflags``
   will be renamed to ``APIClient.feature_flags``.
- The following APIClient attributes have been deprecated in favor of the
  attribute that includes underscores:
  ``APIClient.bocceclusters`` -> ``APIClient.bocce_clusters``
  ``APIClient.matchtargets`` -> ``APIClient.match_targets``
  ``APIClient.remotehosts`` -> ``APIClient.remote_hosts``
- ``civis.io.csv_to_civis`` and ``civis.io.dataframe_to_civis`` functions now use
  ``civis.io.file_to_civis`` and ``civis.io.civis_file_to_table`` functions instead
  of separate logic
- ``civis.io.file_to_civis``, ``civis.io.csv_to_civis`` and ``civis.io.dataframe_to_civis``
  now support files over 5GB
- Refactor internals of ``CivisFuture`` and ``PollableResult`` to centralize handling
  of threads and ``pubnub`` subscription.
- Updated API specification and base resources to include all general
  availability endpoints.
- Changed ``civis.io.file_to_civis`` and ``civis.io.civis_to_file`` to allow
  strings for paths to local files in addition to just file/buffer objects.

### Fixed
- Fixed parsing of multiword endpoints. Parsing no longer removes underscores
  in endpoint names.
- In ``civis.futures.ContainerFuture``, return ``False`` when users attempt to cancel
  an already-completed job. Previously, the object would sometimes give a ``CivisAPIError``
  with a 404 status code. This fix affects the executors and joblib backend, which
  use the ``ContainerFuture``.
- Tell ``flake8`` to ignore a broad except in a ``CivisFuture`` callback.
- Close open sockets (in both the ``APIClient`` and ``CivisFuture``)  when they're no
  longer needed, so as to not use more system file handles than necessary (#173).
- Correct treatment of ``FileNotFoundError`` in Python 2 (#176).
- Fixed parsing of endpoints containing hyphens.  Hyphens are replaced with
  underscores.
- Use ``civis.compat.TemporaryDirectory`` in ``civis.io.file_to_civis`` to be
  compatible with Python 2.7
- Catch notifications sent up to 30 seconds before the ``CivisFuture`` connects.
  Fixes a bug where we would sometimes miss an immediate error on SQL scripts (#174).

### Added
- Documentation updated to include new CivisML features (#137).
- ``civis.resources.cache_api_spec`` function to make it easier to record the
  current API spec locally (#141).
- Autospecced mock of the ``APIClient`` for use in testing third-party code which
  uses this library (#141).
- Added `etl`, `n_jobs`, and `validation_data` arguments to
  ModelPipeline.train (#139).
- Added `cpu`, `memory`, and `disk` arguments to ModelPipeline.predict
  (#139).
- Added ``remote_backend`` keyword to the ``civis.parallel.make_backend_factory``
  and ``civis.parallel.infer_backend_factory`` in order to set the joblib
  backend in the container for nested calls to ``joblib.Parallel``.
- Added the PyPI trove classifiers for Python 3.4 and 3.6 (#152).
- ``civis.io.civis_file_to_table`` function to import an existing Civis file
  to a table
- ``civis.io.file_to_civis`` function will now automatically retry uploads to
  the Civis Platform up to 5 times if is there is an HTTPError, ConnectionError
  or ConnectionTimeout
- Additional documentation about the use case for the Civis joblib backend.
- Added a note about serializing ``ModelPipeline`` ``APIClient`` objects to the docstring.
- Added `civis notebooks download` command-line interface command to facilitate
  downloading notebooks.

### Performance Enhancements
- ``civis.io.file_to_civis`` now takes advantage of multipart uploads to chunk
  files and perform I/O in parallel
- ``civis.io.civis_to_csv`` and ``civis.io.read_civis_sql`` will always request
  data with gzip compression to reduce I/O. Also, they will attempt to fetch
  headers in a separate query so that data can be unloaded in parallel
- ``civis.io.civis_to_csv`` with ``compression='gzip'`` currently returns a file
  with no compression. In a future release, ``compression='gzip'`` will return a
  gzip compressed file.

## 1.6.2 - 2017-09-08
### Changed
- Added explanatory text to CivisML_parallel_training.ipynb (#126).

### Fixed
- Added `ResourceWarning` for Python 2.7 (#128).
- Added `TypeError` for multi-indexed dataframes when used as input to
  CivisML (#131).
- ``ModelPipeline.from_existing`` will warn if users attempt to recreate
  a model trained with a newer version of CivisML, and fall back on the
  most recent prediction template it knows of (#134).
- Make the `PaginatedResponse` returned by LIST endpoints a full iterator.
  This also makes the `iterator=True` parameter work in Python 2.
- When using ``civis.io.civis_to_csv``, emit a warning on SQL queries which
  return no results instead of allowing a cryptic ``IndexError`` to surface (#135).
- Fixed the example code snippet for ``civis.io.civis_to_multifile_csv``.
  Also provided more details on its return dict in the docstring.
- Pinned down `sphinx_rtd_theme` and `numpydoc` in `dev-requirements.txt`
  for building the documentation.

### Added
- Jupyter notebook with demonstrations of use patterns and abstractions in the Python API client (#127).

## 1.6.1 - 2017-08-22
### Changed
- Catch unnecessary warning while importing xgboost in CivisML_parallel_training.ipynb (#121)

### Fixed
- Fixed bug where instantiating a new model via ``ModelPipeline.from_existing`` from an existing model with empty "PARAMS" and "CV_PARAMS" boxes fails (#122).
- Users can now access the ``ml`` and ``parallel`` namespaces from the base ``civis`` namespace (#123).
- Parameters in the Civis API documentation now display in the proper order (#124).

## 1.6.0 - 2017-07-27
### Changed
- Edited example for safer null value handling
- Make ``pubnub`` and ``joblib`` hard dependencies instead of optional dependencies (#110).
- Retry network errors and wait for API rate limit refresh when using the CLI (#117).
- The CLI now provides a User-Agent header which starts with "civis-cli" (#117)
- Include ``pandas`` and ``sklearn``-dependent code in Travis CI tests.

### Added
- Version 1.1 of CivisML, with custom dependency installation from remote git hosting services (i.e., Github, Bitbucket).
- Added email notifications option to ``ModelPipeline``.
- Added custom ``joblib`` backend for multiprocessing in the Civis Platform. Public-facing functions are ``make_backend_factory``, ``make_backend_template_factory``, and ``infer_backend_factory``. Includes a new hard dependency on ``cloudpickle`` to facilitate code transport.

### Fixed
- Fixed a bug where the version of a dependency for Python 2.7 usage was incorrectly specified.
- Non-seekable file-like objects can now be provided to ``civis.io.file_to_civis``. Only seekable file-like objects will be streamed.
- The ``civis.ml.ModelFuture`` no longer raises an exception if its model job is cancelled.
- The CLI's API spec cache now expires after 24 hours instead of 10 seconds.

## 1.5.2 - 2017-05-17
### Fixed
- Fixed a bug where ``ModelFuture.validation_metadata`` would not source training job metadata for a ``ModelFuture`` corresponding to prediction job (#90).
- Added more locks to improve thread safety in the ``PollableResult`` and ``CivisFuture``.
- Fix issue with Python 2/3 dependency management (#89).

## 1.5.1 - 2017-05-15
### Fixed
- Fixed a bug which caused an exception to be set on all ``ModelFuture`` objects, regardless of job status (#86).
- Fixed a bug which made the ``ModelPipeline`` unable to generate prediction jobs for models trained with v0.5 templates (#84).
- Handle the case when inputs to ``ModelFuture`` are ``numpy.int64`` (or other non-``integer`` ints) (#85).

### Changed
- Convert `README.md` (Markdown) to `README.rst` (reStructuredText).

## 1.5.0 - 2017-05-11
### Added
- Retries to http request in ``get_swagger_spec`` to make calls to ``APIClient`` robust to network failure
- Parameter ``local_api_spec`` to ``APIClient`` to allow creation of client from local cache
- Clarify ``civis.io.dataframe_to_civis`` docstring with a note about treatment of the index.
- Added functions ``civis.io.file_id_from_run_output``, ``civis.io.file_to_dataframe``, and ``civis.io.file_to_json``.
- Added ``civis.ml`` namespace with ``ModelPipeline`` interface to Civis Platform modeling capabilities.
- Added ``examples`` directory with sample ``ModelPipeline`` code from ``civis.ml``.
- Python 2.7 compatibility

### Fixed
- Corrected the defaults listed in the docstring for ``civis.io.civis_to_multifile_csv``.
- Do not allow uploading of files greater than 5GB to S3 (#58).
- Revised example code of docstring of civis_to_file to use bytes when downloading file

### Changed
- Modified retry behavior so that 413, 429, or 503 errors accompanied by a "Retry-After" header will be retried regardless of the HTTP verb used.
- Add CSV settings arguments to ``civis.io.civis_to_csv`` function.
- Refactored use of "swagger" language.  ``get_swagger_spec`` is now ``get_api_spec`` and ``parse_swagger`` is now ``parse_api_spec``.
- Modified ``CivisFuture`` so if PubNub is disconnected, it will fall back to polling on a shorter interval.

## 1.4.0 - 2017-03-17
### API Changes
- Deprecate ``api_key`` input to higher-level functions and classes in favor of an ``APIClient`` input. The ``api_key`` will be removed in v2.0.0. (#46)

### Fixed
- Improved threading implementation in ``PollableResult`` so that it no longer blocks interpreter shutdown.
- Allow the base url of the API to be configured through the ``CIVIS_API_ENDPOINT`` environment variable. (#43)

### Added
- Decorator function for deprecating parameters (#46)

## 1.3.0 - 2017-03-07
### Added
- `civis.futures.CivisFuture` for tracking future results

### Performance Enhancements
- ``civis.io.file_to_civis`` will perform a streaming upload to Platform if the optional ``requests-toolbelt`` package is installed.
- Replace all ``PollableResult`` return values with ``CivisFuture`` to reduce the number of API calls and increase speed

## 1.2.0 - 2017-02-08
### Added
- support for multifile csv exports
- support for subscription based polling

### Changed
- civis.io functions use the "hidden" API option to keep jobs out of the UI. Deprecate the "archive" parameter in favor of "hidden".
- civis.io.query_civis now has a "hidden" parameter which defaults to True
- expose `poller` and `poller_args` as public attributes in `PollableResults`
- update to  `default_credential` to handle pagination in `credentials.list` endpoint.

### Fixed
- miscellaneous documentation fixes
- unexpected keyword arguments passed to `APIClient` methods now raise appropriate TypeError

### Performance Enhancements
- Decrease time required to create client objects from ~0.6 seconds to ~150 us for all objects after the first in a session

## 1.1.0 - 2016-12-09
### Changed
- civis.io reads/writes to/from memory instead of disk where appropriate
- Minor documentation corrections

### Fixed
- 204/205 responses now return valid Response object

## 1.0.0 - 2016-11-07
### Added
- Initial release
