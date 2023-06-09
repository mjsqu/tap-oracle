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

# Get a SQLAlchemy engine and run get_schema_names
connector = oracleConnector(SAMPLE_CONFIG)
engine = connector.create_engine()
inspected = sqlalchemy.inspect(engine)
for schema_name in connector.get_schema_names(engine, inspected):
    if 'sys' in schema_name:
        continue
    for table_name, is_view in connector.get_object_names(
                engine,
                inspected,
                schema_name,
            ):
        print(f"{schema_name}.{table_name}")

# Run standard built-in tap tests from the SDK:
#TestTaporacle = get_tap_test_class(
#    tap_class=Taporacle,
#    config=SAMPLE_CONFIG,
#)
