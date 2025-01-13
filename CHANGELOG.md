# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 2.4.3 - 2025-01-13

### Fixed
- Fixed the way array query parameters are passed to a Civis API call,
  so that all items in an array are included in the correctly formatted URL. (#507)

## 2.4.2 - 2025-01-02

### Changed
- Refactored `docs/` since the `docs/source/` subdirectory is no longer needed. (#506)

### Fixed
- Fixed the tool for checking if the upstream Civis API spec has changed. (#505)

### Security
- Bumped the transitive dependency `jinja2`'s version from 3.1.4 to 3.1.5,
  due to CVE-2024-56326. (#506)

## 2.4.1 - 2024-11-27

### Changed
- Updated the Civis API spec in order to refresh the Sphinx docs on the Read The Docs site. (#504)

## 2.4.0 - 2024-11-11

### Added
- The new kwarg `retries` has been added to `civis.APIClient` so that
  a `tenacity.Retrying` instance can be provided to customize retries. (#495)
- Added `civis.workflows.validate_workflow_yaml`
  to validate a Civis Platform workflow YAML definition. (#497, #499)
- The helper I/O functions that create a Civis file
  (i.e., `civis.io.file_to_civis`, `civis.io.dataframe_to_file`, and `civis.io.json_to_file`)
  accept a new `description` keyword argument for the new `description` attribute
  of Civis file objects. (#498, #500)
- `Response` objects are now fully typed through the attribute syntax. (#501)
- Both `Response` and `PaginatedResponse` are now directly available under the `civis` namespace. (#501)
- Added support for Python 3.13. (#501)
- Added the new property `default_database_credential_id` at `civis.APIClient`,
  which is going to replace the existing `default_credential`. (#502)

### Changed
- When a `PaginatedResponse` object is returned from an API call,
  a user-specified `limit` kwarg is now honored to facilitate speeding up the pagination. (#501)

### Deprecated
- The method `get_database_credential_id` at `civis.APIClient` has been deprecated
  and will be removed at civis-python v3.0.0. There's no replacement for this method. (#502)
- The property `default_credential` at `civis.APIClient` has been deprecated
  and will be removed at civis-python v3.0.0,
  in favor of the new property `default_database_credential_id`. (#502)

### Removed
- Dropped support for Python 3.9. (#499)

### Fixed
- The repr form of `Response` objects is now the dict-based `Response({‘spam’: 123})`
  instead of the dataclass-based `Response(spam=123)`, since response object keys can
  be invalid Python identifiers. (#501)
- In `Response` object instantiation, object keys that originate from environment variables
  are now preserved for their (customarily upper-) case even in the default snake-case setting. (#501)
- In `Response` object instantiation, an API response that represents a JSONValue object
  now has its `value` attribute unmodified as the Python object representation
  of the deserialized JSON form (as opposed to being converted to a `Response`-based form). (#501)

## 2.3.0 - 2024-06-14

### Added
- Added a script for checking if the Civis API spec is up-to-date. (#489)
- Added a new keyword argument `sql_params_arguments` to the `civis.io.*` functions that
  accept a SQL query, so that the user can run a parameterized SQL script. (#493)

### Changed
- Refactored the `civis.parallel` module and related unit tests due to major changes
  of joblib from v1.2.0 to v1.3.0 (API-breaking changes for dropping
  `joblib.my_exceptions.TransportableException` and `joblib.format_stack.format_exc`,
  as well as the substantial changes to the internals of `joblib.Parallel`). (#488)
- Bumped the minimum required version of `joblib` to v1.3.0,
  which is the version where `joblib.parallel_config` was introduced and
  `joblib.parallel_backend` was deprecated. (#488)
- Improved the startup time of `import civis` with a 5x speed boost. (#490, #493)
- The downloaded API spec due to the `civis.APIClient` instantiation is now
  a time-to-live cache in memory (15 minutes for interactive Python, or 24 hours in scripts). (#491)
- Polling at `PollableResult` (and consequently its subclasses as well: `CivisFuture`,
  `ContainerFuture`, and `ModelFuture`) now defaults to geometrically increased polling
  intervals. Short-running jobs' `future.result()` can now return faster, while
  longer-running jobs have a capped polling interval of 15 seconds. (#492)
- Comparing a `Response` object with a non-`Response` object returns `False` now
  (this previously raised a `TypeError`). (#493)

### Fixed
- Fixed `civis.parallel.make_backend_template_factory` so that
  keyword arguments are now accepted and passed to `client.scripts.post_custom`. (#488)
- For `Response` objects, their "repr" form shows the class name "Response" for both
  top-level and nested response objects. (#493)

### Security
- Bumped the minimum required version of `requests` to the latest v2.32.3, 
  due to a security vulnerability for < v2.32.0
  ([CVE-2024-35195](https://nvd.nist.gov/vuln/detail/CVE-2024-35195)). (#488)

## 2.2.0 - 2024-05-28

## Added
- `civis.response.Response` has its own "repr" and pretty-print format,
  instead of the previous dict-like representation that would incorrectly suggest immutability. (#487)
- Added the `--version` flag to the command line interface. (#487)

## Fixed
- Fixed API response objects' `.json()` for lists. (#487)
- Fixed `civis_logger` for always having the attribute `propagate` attribute set to `False`
  so that it can also be used for notebooks and services/apps on Civis Platform. (#487)

## 2.1.0 - 2024-05-23

### Added
- Added `.json()` at `civis.response.Response` to return the original JSON data from Civis API. (#486)

### Changed
- Updated the Civis API spec. (#486)

### Fixed
- Fixed `civis.response.Response` so that keys that shouldn't be mutated for casing,
  specifically those under `"arguments"`, are now kept unchanged. (#486)

## 2.0.0 - 2024-05-21

### Breaking Changes from v1.x.x to v2.0.0

(Changes documented in this section are not repeated in the following sections.)

- A `civis.response.Response` object is no longer mutable
  (implementationally, it subclassed `dict` before, which is no longer the case).
  More concretely, both the "setitem" (e.g., `response["foo"] = "bar"`)
  and "setattr" (e.g., `response.foo = "bar"`) operations
  would now raise an `CivisImmutableResponseError` exception. (#463)
- Instantiating a `civis.response.Response` object no longer
  accepts the boolean `snake_case` keyword argument;
  snake-case keys at a `Response` object are now always available (and preferred). (#463)
- Parameters for various classes/functions that have long been deprecated are removed:
  `api_key`, `resources`, `retry_total`, `archive`, `headers`.
  Also dropped the deprecated methods in `ServiceClient`. (#472)
- The `return_type` parameter of a `civis.response.Response` object
  no longer has the `"pandas"` option. (#473)
- When `civis.find` uses kwargs as filters, boolean values are now treated in the same
  way as other data types for value equality comparison, rather than the presence or
  absence of the key in question. (#474)
- To access the API endpoints "MatchTargets" and "RemoteHosts" via `client = civis.APIClient()`,
  `client.matchtargets` and `client.remotehosts` are no longer available.
  Only the names with underscores, i.e., `client.match_targets` and `client.remote_hosts`,
  can be used instead. (#479)

### Added
- Added error handling of file_id with type string passed to `civis.io.civis_file_to_table`. (#454)
- Added support for Python 3.10, 3.11, and 3.12 (#462, #475)
- A `FutureWarning` is now raised when a deprecated Civis API endpoint method is called. (#477)
- Added `civis_logger` for logging in Civis Platform scripts. (#478)
- Added the stub file `client.pyi` to surface the API endpoints and their type annotations
  at a `civis.APIClient` instance to IDEs. (#479)
- Added the `job_url` property to `CivisFuture` objects. (#482)
- Added `.readthedocs.yaml` to configure the ReadTheDocs build. (#483)

### Changed
- Updated references from 'master' to 'main' (#460)
- Clarified the usage example for `civis.io.civis_to_multifile_csv`. Updated 
  CircleCI config so dev-requirements is only used when needed. (#452)
- Removed unneeded `time.sleep` calls and `pytest.mark` calls and mocked `time.sleep` calls to optimize tests. (#453)
- Refactored tests to remove dependency on the vcr library. (#456)
- Fixed typo in "Testing Your Code" example of the User Guide (#458)
- Adding `try`-`except` to catch `JSONDecodeErrors` in `CivisAPIError` (#459)
- `civis.io.file_id_from_run_output` now works for all job types (#461)
- A nested `civis.response.Response` object now supports both snake-case and camel-case
  for key access. Previously, only the non-Pythonic camel-case keys were available. (#463)
- Pinned the dependency `joblib` at `< 1.3.0`, since `joblib >= 1.3.0` is incompatible
  with the current civis-python codebase. (#469)
- Changed `civis.io.civis_file_to_table` to not rely on table IDs for determining a table's existence (#470)
- Broke out the "API Resources" documentation page into individual endpoint pages (#471)
- Switched to `pyproject.toml` for packaging. (#475)
- CI builds for Windows switched from AppVeyor to CircleCI. (#480)
- Applied the `black` code formatter to the codebase. (#481)

### Removed
- Dropped support for Python 3.7 and 3.8 (#462, #475)

### Security
- Added the `pip-audit` check to CI
  for potential security vulnerabilities of Python dependencies. (#476, #485)

## 1.16.1 - 2023-07-10
### Changed
- Changed `civis.io.civis_file_to_table` to not rely on table IDs for determining a table's existence (#464)

## 1.16.0 - 2021-12-14
### Added
- Added documentation around testing code using mocking (#447)
- Added the type of `civis.response.Response` and `civis.response.PaginatedResponse`
  returned in the API resources documentation (#438)
- Added job ID and run ID as custom headers in API calls (#437)
- Added support for Python 3.9 (#436)
- Added job ID and run ID to the exception message of `CivisJobFailure`
  coming from a `CivisFuture` object (#426)
- Added the `encoding` parameter to both `civis.io.read_civis` and `civis.io.read_civis_sql`,
  so that these two functions can retrieve non-UTF-8 data when `use_pandas` is `False`. (#424)
- `ContainerFuture` propagates error messages from logs (#416)
- Added EmptyResultError to `civis.io.read_civis` docs (#412)
- Added default values from swagger in client method's signature (#417)

### Changed
- Added a warning message when using `civis.io.file_to_civis` with file size of 0 bytes (#451)
- Specified that `civis.io.civis_file_to_table` can handle compressed files (#450)
- Explicitly stated CSV-like civis file format requirement in 
  `civis.io.civis_file_to_table`'s docstring (#445)
- Called out the fact that `joblib.Parallel`'s `pre_dispatch` defaults to `"2*n_jobs"`
  in the Sphinx docs (#443)
- Updated `civis_api_spec.json`, moved it to under `civis/resources/`, and checked in
  a script to facilitate updating it again (#440, #441)
- Bumped version numbers for dependencies to allow their latest major releases (#436)
- Switched from TravisCI to CircleCI (#432)
- Moved the changes from #416 for propagating error messages
  from `ContainerFuture` to `CivisFuture` (#426)
- Updated the docstrings for `file_to_civis` (for `buf` and `expires_at`),
  `dataframe_to_file` (for `expires_at`), and `json_to_file` (for `expires_at`). (#427)
- Ability to use joblib 1.1.x (#429)

### Fixed
- Relaxed SQL type checking in `civis.io.civis_file_to_table` by casting to `VARCHAR`
  when type inconsistency is detected for a given column and at least one input file
  has `VARCHAR` (#439)
- Updated info about MacOS shell configuration file to be `~/.zshrc` (#444)
- Fixed the Sphinx docs to show details of multi-word API endpoints (#442)
- Dropped the buggy/unnecessary `_get_headers` in `civis.io.read_civis_sql` (#415) 
- Clarified the `table_columns` parameter in `civis.io.*` functions (#434)
- Warned about the `retry_total` parameter of `civis.APIClient` being inactive and deprecated (#431)
- Converted `assert` statements in non-test code into proper error handling (#430, #435)
- Handled the index-out-of-bounds error when CSV preprocessing fails in `civis_file_to_table`
  by raising a more informative exception (#428)
- Corrected camel to snake case for "sql_type" in `io` docstrings, and added an input check to catch misspellings in the `table_columns` input (#419).

### Removed
- Dropped support for Python 3.6 (#436)
- Removed no-longer-used PubNub code (#425)
- Removed no-longer-supported Python 2 option for notebook creation in the CLI (#421)

### Security
- Turned on `safety` and `bandit` checks at CircleCI builds (#446)

## 1.15.1 - 2020-10-28
### Fixed
- fixes bug whereby calls with iterate=True do not retry (#413)

## 1.15.0 - 2020-09-29
### Changed
- Bump minimum pubnub version to `4.1.12` (#397)
- In `civis.io.civis_file_to_table`, ensure that data types are detected when table_columns are provided with no sql_types. Additionally, throw an error if some sql_types are provided and not others. (#400)
- Retain specific sql types when there are multiple input files and `table_columns` specified in `civis.io.civis_file_to_table` (#402)
- Removed Python 3.5 support (#404)
- Updated list of base API resources to include `aliases`, `git_repos`, `json_values`, `services`, and `storage_hosts` so that they show up in the sphinx docs (#406)
- Update the API spec at `civis/tests/civis_api_spec.json` so that new endpoints are included (e.g., `/exports/files/csv`) (#407)
- Refactor file cleaning logic for `civis.io.civis_file_to_table` (#405)
- Refactored retry logic to use tenacity package, added random jitter on retries, and retry on POST 429 and 503s. (#401)

### Fixed
- Fixed a workflows usage example in `docs/source/client.rst` that had an incorrect endpoint. (#409)
- Fixed a bug in parsing responses that included "update" as a key (e.g., in column information from `client.tables.get(...)`). (#410)

## 1.14.2 - 2020-06-03
### Added
- Added support for Python 3.8 (#391)

### Fixed
- Fixed a bug in the CLI tool which caused failed commands to exit with a 0 exit status. (#389)
- Fixed some issues that the newly-released flake8 3.8 complained about, including a buggy print statement for logging in run_joblib_func.py. (#394)
- Fixed a bug when cancelling jobs while using the Civis joblib backend. (#395)

### Changed
- Added additional detail to `civis.io.dataframe_to_civis`, `civis.io.csv_to_civis`, and `civis.io.civis_file_to_table`'s docstrings on the primary key parameter. (#388)
- Made polling threads for Civis futures be daemon threads so that Python processes will shut down properly in Python 3.8 (#391)
- Removed deprecation warning on the `file_id` parameter of `civis.io.civis_file_to_table`. The parameter name will be kept in v2. (#360, #393)
- Show tables of methods for each set of endpoints in the API Resources pages. (#396)

## 1.14.1 - 2020-04-22
### Fixed
- Fixed a bug in the `ServiceClient` where the API root path was not passed when generating classes. (#384)

## 1.14.0 - 2020-04-22
### Added
- Added `.outputs` method to retrieve outputs from `CivisFuture`
  objects. (#381)
- Added `table_columns` parameter to `civis.io.civis_file_to_table`, `civis.io.dataframe_to_civis`, and `civis.io.csv_to_civis` (#379)

### Fixed
- Fixed/relaxed version specifications for click, jsonref, and jsonschema. (#377)

### Removed

- Removed support for Python 2.7 and 3.4. (#378)

### Changed
- No longer require ServiceClient to be instantiated to parse a
  service api spec. (#382)


## 1.13.1 - 2020-03-06
### Added
- Suppressed FutureWarning from sklearn.externals.joblib. (#375)

## 1.13.0 - 2020-03-05
### Added
- Add `civis jobs follow-log` and `civis jobs follow-run-log` CLI commands. (#359)
- Add documentation for follow-log CLI Commands to main docs. (#367)

### Fixed
- Fixed a bug related to duplicating parent job parameters when using `civis.parallel.infer_backend_factory`. (#363)
- Fixed crashing on NULL fields in `civis sql` CLI command. (#366)
- Fixed `hidden` parameter not getting used in `civis.io.civis_file_to_table`. (#364)
- Fixed `effective_n_jobs` to account for `n_jobs=None`, which is a default for the LogisticsRegression in `sklearn=0.22.x`. (#365)
- Fixed crashing on NULL fields in `civis sql` CLI command (#366)
- Fixed a bug related to creating a ModelPipeline from a registered model. (#369)
- Fixed readme and setup.py to appease twine. (#373)

### Changed
- Made repeated invocations of `civis.tests.create_client_mock` faster by caching the real APIClient that the mock spec is based on (#371)

## 1.12.1 - 2020-02-10
### Fixed
- Fixed issue where client did not generate functions for deprecated API endpoints. (#353)
### Changed
- Changed `ServiceClient` to raise `CivisAPIError`. (#355)
- Updated documentation language for CivisML version. (#358)

## 1.12.0 - 2020-01-14
### Added
- Added method `get_storage_host_id` to the APIClient. (#328)
- Added debug logging to some `civis.io` functions. (#325)
- Added `ServiceClient` and `ServiceEndpoint` class. (#343)
- Added new arguments to `civis.io.civis_to_multifile_csv` to expose max_file_size parameter. (#342)

### Fixed
- Removed incorrect "optional" marker for the `sql` argument in I/O
  functions. (#338)
- Raise a more informative exception when calling `file_to_dataframe`
  on an expired file. (#337)
- `ModelPipeline.register_pretrained_model` should persist the user-supplied
  estimator object indefinitely. (#331)
- Fixed requirements.txt listing for `cloudpickle` -- `>=0.2`, not `<=0.2`. (#323)
- Fixed issue in `civis.io.read_civis_sql` when returning data that contains
  double quotes. (#328)
- Fixed issue with pyyaml version for Python 3.4 by requiring pyyaml version <=5.2

### Changed
- Updated cloudpickle and joblib dependencies. (#349)
- CivisML uses platform aliases instead of hard-coded template IDs. (#341, #347)
- CivisML versions and pre-installed packages are documented on Zendesk instead. (#341)
- Issue a `FutureWarning` on import for Python 2 and 3.4 users. (#333,
  #340)
- Pass `headers` and `delimiter` to Civis API endpoint for cleaning files in `civis.io.civis_file_to_table`. (#334)
- Issue a `FutureWarning` on import for Python 2 users. (#333)
- Update the Civis logo in the Sphinx documentation. (#330)
- Allow the `name` arg to be optional in `civis.io.file_to_civis`. (#324)
- Refactor `civis.io.civis_file_to_table` to use a new set of Civis API endpoints for cleaning and importing CSV files. (#328)
- Added new arguments to `civis.io.civis_file_to_table` to expose additional functionality from new Civis API endpoints. (#328)
- Added new arguments from `civis.io.civis_file_to_table` to `dataframe_to_civis` and `csv_to_civis` methods. (#328)

## 1.11.0 - 2019-08-26
### Added
- Add CLI command "sql" for command line SQL query execution. (#319)
- Add helper function (run_template) to run a template given its id and return
  either the JSON output or the associated file ids. (#318)
- Add helper function to list CivisML models. (#314)
- Added helper functions to share CivisML models with users or groups,
  patterned after the existing API sharing endpoints. (#315)
- Allow the base URL of the CLI to be configured through the
  `CIVIS_API_ENDPOINT` environment variable, like the civis Python module. (#312)
- Allow the CLI log level to be configured with the `CIVIS_LOG_LEVEL`
  environment variable with the standard `logging` module levels.
  For example: `CIVIS_LOG_LEVEL=DEBUG civis users list-me` (#312)
- Allow users to access `civis.utils.run_job` after an `import civis`. (#305)
- `civis.io.dataframe_to_file` and `civis.io.json_to_file` convenience functions.
  (#262, #304)
- Add the user's Python version to the User-Agent string. (#255, #301)
- Added a `last_response` parameter to the `APIClient` object. (#153, #302)
- The deprecate_param decorator can take multiple parameter names, to allow
  Python 2.7 compatibility for multiple deprecations. (#311)

### Fixed
- Added missing docs for `json_to_file` and `dataframe_to_file` (#320).
- Fix unintentional dependency on scikit-learn for `parallel` module tests. (#245, #303)
- Deprecate the `headers` parameter of `dataframe_to_civis` and always tell Civis
  whether the import has headers or not, rather than autodetecting. (#263, #307)
- Set `cloudpickle` requirements to <1.2 on Python v3.4. (#309)
- Fixed an issue in the CLI which prevented users from accessing GET /aliases/{id}
  and simultaneously generated a warning message. (#298, #316)

### Changed
- Loosened version requirements of `pyyaml` to include `pyyaml<=5.99`. (#293)
- Loosened version requirement of `jsonref` to include `0.2` to fix a
  DeprecationWarning under Python 3.7. (#295)
- Changed pubnub version requirement in requirements.txt to match setup.py
  (#295)
- Loosened version requirements of `click` to include v7 and `jsonschema`
  to include v3. (#286, #300)
- Surfaced `civis.io.split_schema_tablename` in the Sphinx docs. (#294)
- Loosen `joblib` version requirement to include v0.13 and add code to
  the Civis joblib backend which newer versions of `joblib` can take
  advantage of. Also loosened version requirement on `cloudpickle` to
  include v1. (#296, #299)
- Run all tests in Ubuntu Xenial. (#310)

## 1.10.0 - 2019-04-09
### Added
- `CivisFuture` has the `job_id` and `run_id` property attributes. (#290)

### Fixed
- Polling will treat `None` responses generated by spotty internet connections
  like responses with a non-DONE state. (#289)

## 1.9.4 - 2019-02-28
### Fixed
- `get_table_id` will correctly handle quoted schema.tablename. (#285)
- Fix parsing of empty responses from run cancellation endpoints. (#287)

## 1.9.3 - 2019-02-05
### Fixed
- Correct prediction template id for CivisML 1.0 (#278)
- `civis.ml.ModelFuture.table` checks for primary key before reading in
  data. (#276)

### Added
- Test for Python 3.7 compatibility (#277)

### Changed
- Updated mock API specs (#281)

## 1.9.2 - 2018-12-03
### Fixed
- `civis.io.civis_to_file` will now retry on S3 connection errors (#273)
- Buffers will be reset appropriately on connection failures during
  `civis.io.file_to_civis` (#273)


## 1.9.1 - 2018-11-15
### Fixed
- `_stash_dataframe_as_csv` in `civis/ml/_model.py` now uses a `StringIO`
  object which has the `getvalue` method (required by `pandas` v0.23.1
  if a file-like object is passed into `df.to_csv`). (#259)
- `civis_to_multifile_csv` fully respects the `client` keyword argument

### Added
- Added instructions in the README for adding an API key to a Windows 10
  environment
- Configured Windows CI using AppVeyor. (#258)

### Changed
- Coalesced `README.rst` and `index.rst`. (#254)
- joblib documentation has moved to readthedocs. (#267)

## 1.9.0 - 2018-04-25
### Fixed
- Added more robust parsing for tablename parsing in io.  You may now
  pass in tables like schema."tablename.with.periods".
- Adding in missing documentation for civis_file_to_table
- Include JSON files with pip distributions (#244)
- Added flush to `civis_to_file` when passed a user-created buffer,
  ensuring the buffer contains the entire file when subsequently read.
- Fix several tests in the `test_io` module (#248)
- Travis tests for Python 3.4 are now restricted to pandas<=0.20, the
  last version which supported Python 3.4 (#249)

### Added
- Added a utility function which can robustly split a Redshift schema name
  and table name which are presented as a single string joined by a "." (#225)
- Added docstrings for `civis.find` and `civis.find_one`. (#224)
- Executors in ``futures`` (and the joblib backend, which uses them) will now
  add "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" environment variables
  to the child jobs they create (#236)
- Update default CivisML version to v2.2. This includes a new function
  ``ModelPipeline.register_pretrained_model`` which allows users to train
  a model outside of Civis Platform and use CivisML to score it at scale (#242, #247).
- Added a new parameter ``dvs_to_predict`` to ``civis.ml.ModelPipeline.predict``.
  This allows users to select a subset of a model's outputs for scoring (#241).
- Added `civis.io.export_to_civis_file` to store results of a SQL query
  to a Civis file
- Surfaced `civis.find` and `civis.find_one` in the Sphinx docs. (#250)

### Changed
- Moved "Optional Dependencies" doc section to top of ML docs, and
  added clarifications for pre-defined models with non-sklearn
  estimators (#238)
- Switched to pip install-ing dependencies for building the documentation (#230)
- Added a merge rule for the changelog to .gitattributes (#229)
- Default to "all" API resources rather than "base".
- Updated documentation on algorithm hyperparameters to reflect changes with
  CivisML v2.2 release (#240)

## 1.8.1 - 2018-02-01
### Added
- Added a script for integration tests (smoke tests).

### Fixed
- Added missing string formatting to a log emit in file multipart upload and
  correct ordering of parameters in another log emit (#217)

### Changed
- Updated documentation with new information about predefined stacking
  estimators (#221)
- Updated CivisML 2.0 notebook (#214)
- Reworded output of `civis notebooks new` CLI command (#215)

## 1.8.0 - 2018-01-23
### Added
- Documentation updated to reflect CivisML 2.1 features (#209)
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
- ``ModelFuture`` objects will emit any warnings which occurred during their
  corresponding CivisML job (#204)
- Removed line setting "n_jobs" from an example of CivisML prediction.
  Recommended use is to let CivisML determine the number of jobs itself (#211).
- Update maximum CivisML version to v2.1; adjust fallback logic such that users get
  the most recent available release (#212).

### Fixed
- Restored the pre-v1.7.0 default behavior of the ``joblib`` backend by setting the ``remote_backend``
  parameter default to 'sequential' as opposed to 'civis'. The default of 'civis' would launch additional
  containers in nested calls to ``joblib.Parallel``. (#205)
- If validation metadata are missing, ``ModelFuture`` objects will return ``None``
  for metrics or validation metadata, rather than issuing an exception (#208)
- Allowed callers to pass `index` and `encoding` arguments to the `to_csv` method through `dataframe_to_civis`.

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
