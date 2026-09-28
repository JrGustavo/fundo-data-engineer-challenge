"""Connections to the source (SQL Server) and the warehouse (DuckDB).

Everything is configured through environment variables set in docker-compose.yml,
so the same code runs unchanged inside the container.
"""
import os

import duckdb
import pymssql


def source(database: str | None = None) -> pymssql.Connection:
    """Connect to SQL Server. `database=None` uses the configured source DB."""
    return pymssql.connect(
        server=os.environ["SRC_HOST"],
        port=os.environ.get("SRC_PORT", "1433"),
        user=os.environ["SRC_USER"],
        password=os.environ["SRC_PASSWORD"],
        database=database or os.environ["SRC_DB"],
        autocommit=False,
    )


def warehouse() -> duckdb.DuckDBPyConnection:
    """Open (or create) the local DuckDB file that stands in for BigQuery."""
    return duckdb.connect(os.environ["WAREHOUSE_PATH"])
