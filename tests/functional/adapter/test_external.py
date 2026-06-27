import subprocess
from pathlib import Path

import pytest
from dbt.tests.util import (
    check_relation_types,
    relation_from_name,
    run_dbt,
)

COMPOSE_FILE = Path(".github/docker-compose.iceberg.yml")
COMPOSE_SERVICES = {"starrocks", "rest", "minio", "mc"}


seed_base_csv = """
id,name,some_date
1,Alice,2023-01-01
2,Bob,2023-01-02
3,Charlie,2023-01-03
4,David,2023-01-04
5,Eve,2023-01-05
6,Frank,2023-01-06
7,Grace,2023-01-07
8,Henry,2023-01-08
9,Iris,2023-01-09
10,Jack,2023-01-10
""".lstrip()

external_catalog_table_sql = """
{{
    config(
        materialized = 'table',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
        partition_by = ['some_date'],
    )
}}
select * from {{ ref('base') }}
""".lstrip()


def run_command(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def _running_compose_services():
    result = run_command(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "ps",
            "--services",
            "--filter",
            "status=running",
        ],
        capture_output=True,
        text=True,
    )
    return set(result.stdout.splitlines())


@pytest.fixture(scope="class")
def iceberg_compose_stack():
    already_running = COMPOSE_SERVICES.issubset(_running_compose_services())
    run_command([
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "400",
    ])
    try:
        yield
    finally:
        if not already_running:
            run_command(["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"])


@pytest.fixture(scope="class")
def dbt_profile_target(iceberg_compose_stack):
    return {
        "type": "starrocks",
        "username": "root",
        "password": "",
        "port": 9030,
        "host": "localhost",
    }


def _catalog_exists(project):
    catalogs = project.run_sql("SHOW CATALOGS", fetch="all")
    return any(row[0] == "iceberg_catalog" for row in catalogs)


def _create_iceberg_catalog(project):
    project.run_sql("""
        CREATE EXTERNAL CATALOG iceberg_catalog
        PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.catalog.uri'='http://iceberg-rest:8181',
            'iceberg.catalog.warehouse'='warehouse',
            'aws.s3.access_key'='admin',
            'aws.s3.secret_key'='password',
            'aws.s3.endpoint'='http://minio:9000',
            'aws.s3.enable_path_style_access'='true'
        )
    """)


@pytest.fixture(scope="class")
def ensure_iceberg_catalog(project):
    if _catalog_exists(project):
        yield
        return

    created_catalog = False
    try:
        _create_iceberg_catalog(project)
        created_catalog = True
        yield
    finally:
        if created_catalog:
            project.run_sql("DROP CATALOG IF EXISTS iceberg_catalog")


class TestExternalCatalogTable:
    """Test basic table materialization in external catalog"""

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(cls):
        return {
            "base.csv": seed_base_csv,
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(cls):
        return {
            "external_table.sql": external_catalog_table_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def setup_external_catalog(cls, project, ensure_iceberg_catalog):
        """Create external database with location"""
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
        """)
        yield

        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_test_db.external_table")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    def test_external_catalog_table(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt()
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "external_table")
        result = project.run_sql(
            f"select count(*) as num_rows from iceberg_catalog.dbt_test_db.{relation.identifier}",
            fetch="one"
        )
        assert result[0] == 10

        # Verify it's actually a table
        expected = {
            "base": "table",
            "external_table": "table",
        }
        check_relation_types(project.adapter, expected)
