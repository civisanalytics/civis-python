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


## Getting Started with Local Development

1. Locally, create an isolated Python environment and activate it
   (e.g., using the built-in [venv](https://docs.python.org/3/tutorial/venv.html)).
2. [For non-Civis employees only] Fork the civis-python repo ( https://github.com/civisanalytics/civis-python/fork ).
3. Clone the civis-python repo to your local drive:

```bash
# For non-Civis employees -- replace <github-username> with your own, as you're cloning from your fork
git clone https://github.com/<github-username>/civis-python.git

# For Civis employees -- you should already have your SSH key set up locally and need git@ to push to this repo directly
git clone git@github.com:civisanalytics/civis-python.git
```

4. Install civis-python in the editable mode, and install the development dependencies as well.

```bash
cd civis-python
pip install -r dev-requirements.txt
pip install -e .
```

5. Make sure you are able to run the test suite locally (`pytest civis`).
6. Create a feature branch (`git checkout -b my-new-feature`).
7. Make your change. Don't forget adding or updating tests.
8. Make sure the test suite, including your new tests, passes
   (`pytest civis && flake8 civis`).
9. Commit your changes (`git commit -am 'Add some feature'`).
10. Push to the branch (`git push origin my-new-feature`).
11. Create a new pull request.
12. If the build fails, address any issues.

## Tips

- All pull requests must include test coverage. If you’re not sure how to test
  your changes, feel free to ask for help.
- Contributions must conform to the guidelines encoded by `flake8`, based on
  PEP-8.
- Don’t forget to add your change to the [CHANGELOG](CHANGELOG.md). See
  [Keep a CHANGELOG](http://keepachangelog.com/) for guidelines.

Thank you for taking the time to contribute!
