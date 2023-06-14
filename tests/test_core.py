"""Tests standard tap features using the built-in SDK tests library."""

import os

from singer_sdk.testing import get_tap_test_class
import sqlalchemy

from tap_oracle.tap import Taporacle
from tap_oracle.client import oracleConnector

SAMPLE_CONFIG = {
    "host":os.getenv('TAP_ORACLE_HOST'),
    "port":os.getenv('TAP_ORACLE_PORT'),
    "user":os.getenv('TAP_ORACLE_USER'),
    "password":os.getenv('TAP_ORACLE_PASSWORD'),
    "service_name":os.getenv('TAP_ORACLE_SERVICE_NAME'),
}

# Run standard built-in tap tests from the SDK:
TestTaporacle = get_tap_test_class(
    tap_class=Taporacle,
    config=SAMPLE_CONFIG,
)
