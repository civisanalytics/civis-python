from glob import glob
import os
import re
import setuptools
from setuptools import find_packages, setup

CLASSIFIERS = [
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
]


if int(setuptools.__version__.split(".", 1)[0]) < 18:
    raise AssertionError("setuptools >= 18 must be installed")


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


def main():
    with open('README.rst') as README_FILE:
        README = README_FILE.read()

    setup(
        classifiers=CLASSIFIERS,
        name="civis",
        version=get_version(),
        author="Civis Analytics Inc",
        author_email="opensource@civisanalytics.com",
        url="https://www.civisanalytics.com",
        description="Access the Civis Platform API",
        packages=find_packages(),
        data_files=[(os.path.join('civis', 'tests'),
                     glob(os.path.join('civis', 'tests', '*.json')))],
        long_description=README,
        install_requires=[
            'pyyaml>=3.0,<=3.99',
            'click>=6.0,<=6.99',
            'jsonref>=0.1.0,<=0.1.99',
            'requests>=2.12.0,==2.*',
            'jsonschema>=2.5.1,==2.*',
            'six>=1.10,<=1.99',
            'joblib>=0.11,<=0.11.99',
            'pubnub>=4.0,<=4.99',
            'cloudpickle>=0.2.0,<=0.99999',
        ],
        extras_require={
            ':python_version=="2.7"': [
                'funcsigs==1.0.2',
                'future>=0.16,<=0.99',
                'futures==3.1.1',
                'functools32>=3.2,<=3.99'
            ],
        },
        entry_points={
            'console_scripts': [
                'civis = civis.cli.__main__:main',
                'civis_joblib_worker = civis.run_joblib_func:main',
            ]
        }
    )


if __name__ == "__main__":
    main()
