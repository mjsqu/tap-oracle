"""Tests standard tap features using the built-in SDK tests library."""

import os
import re

from sqlalchemy import text

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

sample_repo_path = f"os.getenv('GITHUB_WORKSPACE')/oracle-samples/oracle-samples/db-sample-schemas"

def setup_hr_user(con):
    #con.execute(text('DROP USER hr CASCADE'))
    query = f"""CREATE USER hr IDENTIFIED BY {os.getenv('TAP_ORACLE_PASSWORD')}
                DEFAULT TABLESPACE USERS
                QUOTA UNLIMITED ON USERS"""
    con.execute(text(query))
    query = f"""GRANT CREATE MATERIALIZED VIEW,
      CREATE PROCEDURE,
      CREATE SEQUENCE,
      CREATE SESSION,
      CREATE SYNONYM,
      CREATE TABLE,
      CREATE TRIGGER,
      CREATE TYPE,
      CREATE VIEW
      TO hr"""
    con.execute(text(query))

with engine.connect() as con:
    setup_hr_user(con)
    con.execute(text("""ALTER SESSION SET CURRENT_SCHEMA=HR"""))
    with open(f"{sample_repo_path}/human_resources/hr_create.sql",'r') as f:
        clean_statements = re.sub(r'(?mi)^(rem|prompt|set).*\n?','',f.read())
        # Extra clean up required for semicolons in strings
        # This looks for a semicolon followed by a non-newline space, followed by a non-newline
        clean_statements = re.sub(r";([^\S\r\n][^\n])",r'\1',clean_statements)

    for stmt in clean_statements.split(';'):
        print(f"{stmt}")
        if stmt.strip() != '':
            con.execute(text(stmt))

    with open(
        f"{sample_repo_path}/human_resources/hr_populate.sql"
    ,'r') as f:
        # For this file, grab anything between BEGIN/END;
        pattern = re.compile(r'(?s)(BEGIN.*?END;|ALTER TABLE.*?dept_mgr_fk)')
        for statement in re.findall(pattern, f.read()):
            con.execute(text(statement))

    con.execute(text("""COMMIT"""))
