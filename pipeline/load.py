"""Incremental loader: SQL Server -> DuckDB (the local stand-in for BigQuery).

    python -m pipeline.load            # initial load the first time, incremental afterwards
    python -m pipeline.load status     # what the warehouse knows: versions, last runs

Strategies (chosen from the numbers in `make measure`, see SOLUTION.md):

  change_tracking  Customers, Advances, Transactions, Cards
                   SQL Server Change Tracking returns the NET change per primary key since
                   the last synced version: inserts, updates (even when the app forgot to
                   bump UpdatedAt) and deletes. We fetch the current row for every changed
                   key; keys whose row no longer exists are deletes.
  append_only      CustomerHistory
                   Rows are never updated or deleted, so a high-water mark on HistoryId is
                   enough. We re-read a small overlap below the mark because identity values
                   can commit out of order; delete+insert makes the overlap harmless.

Reliability guarantees:
  * Consistent extract: every table is read inside ONE snapshot-isolation transaction on the
    source, together with CHANGE_TRACKING_CURRENT_VERSION(), so versions and rows agree.
  * Atomic apply: per table, the row changes AND the new watermark are written in ONE DuckDB
    transaction. A crash leaves the table exactly as before and the watermark untouched.
  * Idempotent: changes are applied as delete-then-insert by primary key, so replaying the
    same window (after a crash, or running twice) converges to the same state.
  * Self-healing: if a watermark is older than Change Tracking's retention, that table is
    fully resynced instead of silently missing changes.

Set CRASH_AT=<table> to simulate a failure in the middle of applying that table.
"""
import os
import sys
import time
import uuid
from datetime import datetime

from pipeline import config
from pipeline.arrow import to_arrow
from pipeline.tables import APPEND_ONLY, CHANGE_TRACKING, TABLES, Table

APPEND_OVERLAP = 100   # re-read ids below the high-water mark (out-of-order identity commits); tuned from 500 after measuring its cost

META_DDL = [
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS meta",
    "CREATE SCHEMA IF NOT EXISTS clean",
    """CREATE TABLE IF NOT EXISTS meta.load_state (
        table_name VARCHAR PRIMARY KEY, strategy VARCHAR,
        last_version BIGINT,      -- change_tracking: last synced CT version
        last_id BIGINT,           -- append_only: high-water mark
        updated_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS meta.load_log (
        run_id VARCHAR, table_name VARCHAR, mode VARCHAR, from_mark BIGINT, to_mark BIGINT,
        rows_upserted BIGINT, rows_deleted BIGINT, payload_bytes BIGINT, seconds DOUBLE,
        status VARCHAR, finished_at TIMESTAMP)""",
]

# Clean layer: fixes the bad schema choice (text identifier) without touching the source.
CLEAN_VIEWS = [
    """CREATE OR REPLACE VIEW clean.transactions AS
       SELECT TransactionId, TRY_CAST(TRIM(AdvanceRef) AS INTEGER) AS AdvanceId, AdvanceRef AS AdvanceRefRaw,
              CustomerId, Type, Amount, Status, CreatedAt
       FROM raw.Transactions""",
]


# ---------------------------------------------------------------- source side
def ensure_change_tracking(db: str) -> None:
    """Idempotent source migration: snapshot isolation + Change Tracking on the CT tables."""
    with config.source(autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SELECT snapshot_isolation_state FROM sys.databases WHERE name = %s", (db,))
        if cur.fetchone()[0] != 1:
            cur.execute(f"ALTER DATABASE [{db}] SET ALLOW_SNAPSHOT_ISOLATION ON")
            print("source: enabled snapshot isolation")
        cur.execute("SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()")
        if cur.fetchone()[0] == 0:
            cur.execute(f"ALTER DATABASE [{db}] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON)")
            print("source: enabled Change Tracking on the database (retention 7 days)")
        for t in TABLES.values():
            if t.strategy != CHANGE_TRACKING:
                continue
            cur.execute("SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(%s)", (f"dbo.{t.name}",))
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE dbo.{t.name} ENABLE CHANGE_TRACKING")
                print(f"source: enabled Change Tracking on dbo.{t.name}")


def extract(cur, t: Table, state: tuple | None, current_version: int):
    """Return (mode, from_mark, to_mark, rows_to_upsert, keys_to_delete) for one table."""
    cols = ", ".join(f"t.{c}" for c in t.names)

    if t.strategy == APPEND_ONLY:
        last_id = state[1] if state and state[1] is not None else None
        if last_id is None:
            cur.execute(f"SELECT {cols} FROM dbo.{t.name} AS t ORDER BY t.{t.pk}")
            mode, frm = "initial", None
        else:
            frm = max(last_id - APPEND_OVERLAP, 0)
            cur.execute(f"SELECT {cols} FROM dbo.{t.name} AS t WHERE t.{t.pk} > %s ORDER BY t.{t.pk}", (frm,))
            mode = "incremental"
        rows = cur.fetchall()
        batch = to_arrow(cur, rows)
        to = max((r[0] for r in rows), default=last_id or 0)
        return mode, frm, to, batch, None

    last_version = state[0] if state else None
    if last_version is not None:
        cur.execute("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(%s))", (f"dbo.{t.name}",))
        min_valid = cur.fetchone()[0]
        if min_valid is not None and last_version < min_valid:
            print(f"  {t.name}: watermark {last_version} is older than CT retention ({min_valid}) -> full resync")
            last_version = None

    if last_version is None:
        cur.execute(f"SELECT {cols} FROM dbo.{t.name} AS t")
        mode = "initial" if state is None else "full_resync"
        return mode, None, current_version, to_arrow(cur, cur.fetchall()), None

    # Net changes since last_version. LEFT JOIN: a key with no current row was deleted.
    cur.execute(f"SELECT ct.{t.pk} AS __key, CASE WHEN t.{t.pk} IS NULL THEN 0 ELSE 1 END AS __exists, {cols} "
                f"FROM CHANGETABLE(CHANGES dbo.{t.name}, %s) AS ct "
                f"LEFT JOIN dbo.{t.name} AS t ON t.{t.pk} = ct.{t.pk}", (last_version,))
    changes = to_arrow(cur, cur.fetchall())
    return "incremental", last_version, current_version, changes, True


# ---------------------------------------------------------------- warehouse side
def apply(wh, t: Table, mode: str, to_mark: int, batch, is_changeset: bool | None, run_id: str) -> tuple[int, int]:
    """Apply one table's changes and its new watermark atomically."""
    names = ", ".join(t.names)
    wh.register("batch", batch)
    wh.begin()
    try:
        if mode in ("initial", "full_resync"):
            wh.execute(f"DELETE FROM raw.{t.name}")
            deleted = 0
            wh.execute(f"INSERT INTO raw.{t.name} SELECT {names}, ?, now() FROM batch", [to_mark])
            upserted = batch.num_rows
        elif is_changeset:   # change tracking: delete every changed key, re-insert the ones that still exist
            deleted = wh.execute(f"SELECT COUNT(*) FROM raw.{t.name} WHERE {t.pk} IN "
                                 f"(SELECT __key FROM batch WHERE __exists = 0)").fetchone()[0]
            wh.execute(f"DELETE FROM raw.{t.name} WHERE {t.pk} IN (SELECT __key FROM batch)")
            if os.environ.get("CRASH_AT") == t.name:
                raise RuntimeError(f"SIMULATED CRASH while applying {t.name} (changes half-applied inside the transaction)")
            wh.execute(f"INSERT INTO raw.{t.name} SELECT {names}, ?, now() FROM batch WHERE __exists = 1", [to_mark])
            upserted = wh.execute("SELECT COUNT(*) FROM batch WHERE __exists = 1").fetchone()[0]
        else:                # append-only: replay the overlap window safely
            wh.execute(f"DELETE FROM raw.{t.name} WHERE {t.pk} IN (SELECT {t.pk} FROM batch)")
            wh.execute(f"INSERT INTO raw.{t.name} SELECT {names}, ?, now() FROM batch", [to_mark])
            upserted, deleted = batch.num_rows, 0

        version, last_id = (None, to_mark) if t.strategy == APPEND_ONLY else (to_mark, None)
        wh.execute("INSERT OR REPLACE INTO meta.load_state VALUES (?, ?, ?, ?, now())", [t.name, t.strategy, version, last_id])
        wh.commit()
        return upserted, deleted
    except Exception:
        wh.rollback()
        raise
    finally:
        wh.unregister("batch")


def log(wh, run_id, t, mode, frm, to, ups, dels, nbytes, secs, status) -> None:
    wh.execute("INSERT INTO meta.load_log VALUES (?,?,?,?,?,?,?,?,?,?,now())",
               [run_id, t.name, mode, frm, to, ups, dels, nbytes, round(secs, 3), status])


# ---------------------------------------------------------------- entry points
def run() -> None:
    db = os.environ["SRC_DB"]
    ensure_change_tracking(db)
    wh = config.warehouse()
    for ddl in META_DDL:
        wh.execute(ddl)
    for t in TABLES.values():
        wh.execute(t.warehouse_ddl())
    for view in CLEAN_VIEWS:
        wh.execute(view)

    run_id = datetime.now().strftime("%Y%m%dT%H%M%S-") + uuid.uuid4().hex[:6]
    print(f"run {run_id}")
    print(f"  {'table':<17}{'strategy':<17}{'mode':<13}{'from':>8}{'to':>8}{'upserted':>10}{'deleted':>9}{'payload KB':>12}")

    with config.source(autocommit=True) as conn:
        cur = conn.cursor()
        # One consistent point in time for every table: rows and CT version agree.
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
        current_version = cur.fetchone()[0]
        try:
            for t in TABLES.values():
                state = wh.execute("SELECT last_version, last_id FROM meta.load_state WHERE table_name = ?", [t.name]).fetchone()
                started = time.perf_counter()
                mode, frm, to, batch, is_changeset = extract(cur, t, state, current_version)
                try:
                    ups, dels = apply(wh, t, mode, to, batch, is_changeset, run_id)
                except Exception as exc:
                    log(wh, run_id, t, mode, frm, to, 0, 0, batch.nbytes, time.perf_counter() - started, "failed")
                    print(f"  {t.name:<17}FAILED: {exc}")
                    print("  -> nothing from this table was written; its watermark is unchanged. Re-run `make load`.")
                    sys.exit(1)
                secs = time.perf_counter() - started
                log(wh, run_id, t, mode, frm, to, ups, dels, batch.nbytes, secs, "ok")
                print(f"  {t.name:<17}{t.strategy:<17}{mode:<13}{frm if frm is not None else '-':>8}{to:>8}"
                      f"{ups:>10,}{dels:>9,}{batch.nbytes / 1024:>12.1f}")
        finally:
            cur.execute("COMMIT")   # read-only transaction; just releases the snapshot
    wh.close()


def status() -> None:
    wh = config.warehouse()
    print("Watermarks:")
    for row in wh.execute("SELECT table_name, strategy, last_version, last_id, updated_at FROM meta.load_state ORDER BY 1").fetchall():
        print(f"  {row[0]:<17}{row[1]:<17}version={row[2]}  last_id={row[3]}  at {row[4]:%Y-%m-%d %H:%M:%S}")
    print("\nLast 10 table loads:")
    for row in wh.execute("SELECT finished_at, table_name, mode, status, rows_upserted, rows_deleted, payload_bytes "
                          "FROM meta.load_log ORDER BY finished_at DESC LIMIT 10").fetchall():
        print(f"  {row[0]:%H:%M:%S}  {row[1]:<17}{row[2]:<13}{row[3]:<8}up={row[4]:<7}del={row[5]:<5}{row[6] / 1024:>9.1f} KB")
    wh.close()


if __name__ == "__main__":
    status() if sys.argv[1:] == ["status"] else run()
