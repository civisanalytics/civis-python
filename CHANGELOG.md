# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Fixed
- Handle the case when inputs to ``ModelFuture`` are ``numpy.int64`` (or other non-``integer`` ints) (#85).

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
