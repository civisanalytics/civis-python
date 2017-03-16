# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### API Changes
- Deprecate ``api_key`` input to higher-level functions and classes in favor of an ``APIClient`` input. The ``api_key`` will be removed in v2.0.0. (#46)

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
