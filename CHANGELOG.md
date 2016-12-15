# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).


## [Unreleased]
### Changed
- civis.io functions use the "hidden" API option to keep jobs out of the UI. Deprecate the "archive" parameter in favor of "hidden".
- civis.io.query_civis now has a "hidden" parameter which defaults to True
- expose `poller` and `poller_args` as public attributes in `PollableResults`

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
