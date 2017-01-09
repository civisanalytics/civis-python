import re
import os
from setuptools import find_packages, setup


def get_version():
    version = open("civis/_version.py", "r").read()
    # this is certainly not exhaustive for pep 440
    regex = "(?P<major>\d+)(.(?P<minor>\d+))?(.(?P<micro>\d+))?"
    match = re.search(regex, version)
    if not match:
        raise RuntimeError("Unable to find version string.")
    MAJOR = match.group('major')
    MINOR = match.group('minor') or "0"
    MICRO = match.group('micro') or "0"
    return ".".join([MAJOR, MINOR, MICRO])


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as _in:
        return _in.read()


def main():
    with open('requirements.txt') as f:
        required = f.read().splitlines()

    setup(
        name="civis",
        version=get_version(),
        author="Civis Analytics Inc",
        author_email="opensource@civisanalytics.com",
        url="https://www.civisanalytics.com",
        description="Access the Civis Platform API",
        packages=find_packages(),
        long_description=read('README.md'),
        install_requires=required,
        extras_require={
            'pubnub': ['pubnub>=4.0.0,<=4.99']
        },
        entry_points={
            'console_scripts': [
                'civis = civis.cli.__main__:main'
            ]
        }
    )

if __name__ == "__main__":
    main()
