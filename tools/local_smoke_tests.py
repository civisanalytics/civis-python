#!/usr/bin/env python3

"""
This script tests end-to-end functionality using the Civis Python client.
It uses the live Civis API and Redshift, so a valid CIVIS_API_KEY is needed.

This is based on a similar script for the R client:
https://github.com/civisanalytics/civis-r/blob/master/tools/integration_tests/smoke_test.R
"""

import io
import logging
import time

import civis
import pandas as pd
from sklearn.datasets import load_iris


def main():
    logging.basicConfig(format="", level=logging.INFO)
    logger = logging.getLogger("civis")
    logger.info('generating docs...')
    client = civis.APIClient()
    logger.info('printing username...')
    logger.info(client.users.list_me()['username'])


if __name__ == '__main__':
    main()
