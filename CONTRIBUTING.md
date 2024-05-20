# Contributing to civis-python

We welcome bug reports and pull requests from everyone!
This project is intended to be a safe, welcoming space for collaboration, and
contributors are expected to adhere to the
[Contributor Covenant](http://contributor-covenant.org) code of conduct.


## Filing a Ticket

If you'd like to add or update a feature in civis-python,
it is recommended that you first file a ticket to discuss your proposed changes
and check their compatibility with Civis Platform before making a pull request.

To file a ticket:

* [For non-Civis employees only] Please create a [GitHub issue](https://github.com/civisanalytics/civis-python/issues).
* [For Civis employees only] Please file an internal ticket.


## Local Development Set-up

These set-up steps need to be done only once per machine / OS.

1. Locally, create an isolated Python environment and activate it
   (e.g., using the built-in [venv](https://docs.python.org/3/tutorial/venv.html)).
   For the Python version, use the latest Python 3.x that civis-python supports,
   as indicated in `pyproject.toml` at the repo's top level.
2. [For non-Civis employees only] Fork the civis-python repo ( https://github.com/civisanalytics/civis-python/fork ).
3. Clone the civis-python repo to your local drive:

```bash
# For non-Civis employees -- replace <github-username> with your own, as you're cloning from your fork
git clone https://github.com/<github-username>/civis-python.git

# For Civis employees -- you should already have your SSH key set up locally and need git@ to push to this repo directly
git clone git@github.com:civisanalytics/civis-python.git
```

4. Use the name `upstream` to point to the upstream source repo `civisanalytics/civis-python` in `git remote`:

```bash
# For non-Civis employees:
git remote add upstream https://github.com/civisanalytics/civis-python.git

# For Civis employees -- git uses `origin` by default, so change it into `upstream`
git remote rename origin upstream
```

5. Install civis-python in the editable mode, and install the development dependencies as well.

```bash
cd civis-python
pip install -e ".[dev-core,dev-civisml]"
```

## Making Changes

Follow these steps each time you plan to make a pull request to civis-python:

1. At your local civis-python copy, make sure the `main` branch is in sync with the
   `main` at the upstream repo (`git checkout main && git pull upstream main`).
2. Make sure you are able to run the test suite locally (`pytest civis`).
3. Create a feature branch (`git checkout -b my-new-feature`).
4. Make your change. Don't forget adding or updating tests (under `tests/`).
5. Make sure the test suite, including your new tests, passes
   (`pytest && flake8 src tools tests && black --check src tools tests`).
6. Commit your changes (`git commit -am 'Add some feature'`).
7. Push to a branch on GitHub:

```bash
# For non-Civis employees -- your branch will be at your fork
git push origin my-new-feature

# For Civis employees -- your branch will be at the upstream repo
git push upstream my-new-feature
```

8. Create a new pull request on the GitHub interface.
   A civis-python maintainer will be automatically notified and start the code review process.
9. If the build fails, address any issues.

## Tips

- All pull requests must include test coverage. If you’re not sure how to test
  your changes, feel free to ask for help.
- Contributions must conform to the guidelines encoded by `flake8`, based on
  PEP-8.
- Don’t forget to add your change to the [CHANGELOG](CHANGELOG.md). See
  [Keep a CHANGELOG](http://keepachangelog.com/) for guidelines.

Thank you for taking the time to contribute!
