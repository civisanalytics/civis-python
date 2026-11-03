# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Development assumes a `uv` virtual environment on the latest supported Python (the highest version in the trove classifiers in [pyproject.toml](pyproject.toml)), as set up in [CONTRIBUTING.md](CONTRIBUTING.md). Install dev deps with `uv pip install -e ".[dev-core,dev-civisml]"`.

- Run all tests: `pytest` (configured in [pyproject.toml](pyproject.toml) to target `tests/` with coverage on `src/civis` and `--strict-markers`).
- Run a single test: `pytest tests/test_client.py::test_name` or `pytest -k "partial_name"`.
- Tests require `CIVIS_API_KEY` to be set to any value (CI uses `FOOBAR`). The API spec is loaded from the committed [src/civis/resources/civis_api_spec.json](src/civis/resources/civis_api_spec.json), so tests don't call the Civis API. The one exception to "no network" is `test_workflows_public_repo`, which downloads the public `civisanalytics/workflows-public` repo from GitHub.
- Lint and format check (matches CI): `flake8 src tools tests docs/conf.py && black --check src tools tests docs/conf.py`. Auto-format with `black src tools tests docs/conf.py`. Style is `black` at line length 88 and `flake8` with `max-line-length = 88`, `extend-ignore = E203` (see [.flake8](.flake8)).
- Type-check: `mypy src`. Several modules are excluded; see `[tool.mypy]` in [pyproject.toml](pyproject.toml).
- Security scans (CI): `bandit -r src -x tests`, and `pip-audit --skip-editable` after also installing `docs/requirements.txt`.
- Build docs: `uv pip install -r docs/requirements.txt`, then `sphinx-build -b html docs docs/build`. [docs/conf.py](docs/conf.py) generates one page per endpoint (`docs/api_<endpoint>_endpoint.rst`) and one per endpoint method (`docs/api_<endpoint>/<method>.rst`); these are gitignored. Delete `docs/build` when switching between branches that generate different pages, so Sphinx doesn't leave stale HTML behind.
- CLI entrypoint: `civis --help` (defined in [src/civis/cli/__main__.py](src/civis/cli/__main__.py)).

CI ([.circleci/config.yml](.circleci/config.yml)) runs bandit, black, flake8, mypy, pip-audit, and a twine check first, then the test suite on every supported Python version and on Windows.

## Architecture

### Dynamic API client generation

The core abstraction is [APIClient](src/civis/client.py) to interact with the Civis API, but it has no hand-written endpoint methods. `client.scripts.*`, `client.jobs.*`, `client.users.*`, and so on are **generated** from a Civis OpenAPI spec:

- At runtime, `APIClient` uses a Civis API key (typically from the environment variable `CIVIS_API_KEY`, but it can also be passed into `APIClient` through the keyword argument `api_key`) to fetch the Civis OpenAPI spec for the given Civis Platform user.
- Separately, [src/civis/resources/civis_api_spec.json](src/civis/resources/civis_api_spec.json) is the committed OpenAPI spec and the source of truth for which endpoints exist for a standard Civis Platform user. It also drives the "API Resources" pages in the docs.
- [src/civis/resources/_resources.py](src/civis/resources/_resources.py) walks the spec and builds `Endpoint` subclasses with Python methods, docstrings, and signatures, mapping camelCase JSON to snake_case kwargs.
- [src/civis/client.pyi](src/civis/client.pyi) is an **auto-generated** type stub (from [src/civis/resources/_client_pyi.py](src/civis/resources/_client_pyi.py)) that mirrors the dynamic methods for IDEs and `mypy`. Do not hand-edit it.
- [tools/update_civis_api_spec.py](tools/update_civis_api_spec.py) re-downloads the spec and regenerates `client.pyi`, keeping the two in sync. It needs a Civis employee's regular Platform API key in `CIVIS_API_KEY`, because it runs a custom script on Civis Platform to export the spec. The resulting spec only reflects what a standard Platform user can see. After a refresh, tests that hardcode endpoint parameters (e.g., `test_generate_cli_civis` in [tests/test_cli.py](tests/test_cli.py)) may need updating, and the change gets an "Updated the API spec." changelog entry.

Consequences for editing:
- Adding or renaming an endpoint method by hand in `client.py` is almost never the right fix. Changes usually belong in the spec or in `_resources.py` (parsing and naming logic).
- `_resources.py` has workaround sets such as `GET_PATHS_WITH_ARRAY_RESPONSE` and `LEGACY_PATHS_WITH_LIST_NAME`. The latter exists only for backward compatibility with the pre-PR-530 "list" vs. "get" naming and is slated for removal in v3.0.0, so do **not** add new endpoints to it.
- `RESOURCES_TO_EXCLUDE` omits endpoints whose names collide with `APIClient` attributes (e.g., `feature_flags`).

### Package layout

- [src/civis/client.py](src/civis/client.py): `APIClient`, the user entrypoint. Wraps retry config ([src/civis/_retries.py](src/civis/_retries.py)), API key resolution ([src/civis/_get_api_key.py](src/civis/_get_api_key.py)), and dynamic class generation.
- [src/civis/response.py](src/civis/response.py): `Response`, `ListResponse`, `PaginatedResponse`, and the `find`/`find_one` helpers. All endpoint return values flow through these.
- [src/civis/io/](src/civis/io/): high-level helpers for files, tables, and databases (`civis.io.read_civis`, `civis.io.civis_to_csv`, etc.). Dataframe support is a soft dependency on `pandas` or `polars`.
- [src/civis/futures.py](src/civis/futures.py) and [src/civis/polling.py](src/civis/polling.py): `concurrent.futures`-style objects for long-running Civis jobs. Most job helpers return one of these.
- [src/civis/utils/_jobs.py](src/civis/utils/_jobs.py): generic job helpers (e.g., `run_job`).
- [src/civis/cli/](src/civis/cli/): Click-based CLI that surfaces every generated endpoint method, plus custom commands in `_cli_commands.py`.
- [src/civis/workflows/](src/civis/workflows/): `validate_workflow_yaml`, which checks Civis Platform workflow YAML with JSON schemas ([_schemas.py](src/civis/workflows/_schemas.py), partly derived from the API spec's script-creation endpoints) plus cross-task checks ([_validate.py](src/civis/workflows/_validate.py)). It aims to match what Civis Platform accepts.
- Deprecated and scheduled for removal in v3.0.0 (February 2027): [src/civis/ml/](src/civis/ml/) (CivisML; soft deps in the `dev-civisml` extra), [src/civis/parallel.py](src/civis/parallel.py) with [src/civis/run_joblib_func.py](src/civis/run_joblib_func.py) (joblib backend on Civis container scripts, the `civis_joblib_worker` entrypoint), `ContainerFuture`, and [src/civis/service_client.py](src/civis/service_client.py) (`ServiceClient`). Their `FutureWarning`s are filtered in the pytest config.

[src/civis/__init__.py](src/civis/__init__.py) uses `importlib.util.LazyLoader` for the heavier subpackages (`futures`, `io`, `ml`, `parallel`, `utils`, `workflows`) so `import civis` stays cheap. Keep new top-level imports cheap or add them to the lazy list.

### Tests

- Tests under [tests/](tests/) mirror `src/civis/` module by module (`tests/test_client.py` for `src/civis/client.py`, etc.).
- `civis.tests.create_client_mock` is useful when an `APIClient` mock is needed for tests.

### Other maintainer tools

[tools/](tools/) also has `check_if_civis_api_spec_has_updated.py` (run by a scheduled Platform job to flag spec drift) and `smoke_tests.py` (end-to-end checks against the live Civis API and Redshift, which need a real API key). See their docstrings for usage.

## Conventions

- The git remote for `civisanalytics/civis-python` is named `upstream`, not `origin`.
- PR titles start with the internal ticket key and a change-type tag, e.g., `[CIVIS-1234] FIX ...`, using `ENH`, `FIX`, `DEP`, `DOC`, or `MAINT` (see `git log`).
- Branch names are prefixed with the ticket key, e.g., `civis-1234-<short-description>` for the ticket CIVIS-1234.
- Every change gets an entry under "Unreleased" in [CHANGELOG.md](CHANGELOG.md) (Keep a Changelog format), ending with the PR number, e.g., `(#548)`.
- The version lives in `[project].version` in [pyproject.toml](pyproject.toml) and is surfaced as `civis.__version__`. Publishing a GitHub release tagged `vX.Y.Z` publishes to PyPI ([.github/workflows/release.yml](.github/workflows/release.yml)) and triggers the "stable" docs build on Read the Docs.
