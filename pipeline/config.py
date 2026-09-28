"""Connections to the source (SQL Server) and the warehouse (DuckDB).

Everything is configured through environment variables set in docker-compose.yml,
so the same code runs unchanged inside the container.
"""
import os
import re
from pathlib import Path

import duckdb
import pymssql

ROOT = Path(__file__).resolve().parent.parent


def source(database: str | None = None, autocommit: bool = False) -> pymssql.Connection:
    """Connect to SQL Server. `database=None` uses the configured source DB."""
    return pymssql.connect(
        server=os.environ["SRC_HOST"],
        port=os.environ.get("SRC_PORT", "1433"),
        user=os.environ["SRC_USER"],
        password=os.environ["SRC_PASSWORD"],
        database=database or os.environ["SRC_DB"],
        autocommit=autocommit,
    )


def warehouse() -> duckdb.DuckDBPyConnection:
    """Open (or create) the local DuckDB file that stands in for BigQuery."""
    return duckdb.connect(os.environ["WAREHOUSE_PATH"])


def run_sql_file(cur, path: Path) -> None:
    """Execute a T-SQL script, splitting on GO batch separators like sqlcmd does."""
    for batch in re.split(r"^\s*GO\s*$", path.read_text(), flags=re.MULTILINE | re.IGNORECASE):
        if batch.strip():
            cur.execute(batch)


def insert_rows(cur, table: str, columns: list[str], rows: list[tuple], identity: bool = False) -> None:
    """Multi-row INSERT in chunks that respect SQL Server's 2100-parameter limit."""
    if not rows:
        return
    per_stmt = max(1, min(1000, 2000 // len(columns)))
    cols = ", ".join(columns)
    if identity:
        cur.execute(f"SET IDENTITY_INSERT {table} ON")
    for i in range(0, len(rows), per_stmt):
        chunk = rows[i:i + per_stmt]
        values = ", ".join(["(" + ", ".join(["%s"] * len(columns)) + ")"] * len(chunk))
        cur.execute(f"INSERT INTO {table} ({cols}) VALUES {values}", tuple(v for r in chunk for v in r))
    if identity:
        cur.execute(f"SET IDENTITY_INSERT {table} OFF")
