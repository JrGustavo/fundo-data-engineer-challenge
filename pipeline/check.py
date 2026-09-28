"""Is the warehouse complete and correct right now?

    python -m pipeline.check            # compare source vs warehouse, exit 1 on a real problem
    python -m pipeline.check repair     # re-copy only the rows the last check flagged

For every loaded table we compare, key by key, a fingerprint of each row computed on
BOTH sides (SQL Server and DuckDB) from the same canonical text form:

  missing     key in the source, not in the warehouse      (a gap)
  extra       key in the warehouse, not in the source      (a delete that never arrived)
  mismatched  key on both sides, different content         (a stale or corrupted row)

A difference is only a FAILURE if the loader could not explain it. Keys that changed in
the source after the last load (Change Tracking since the watermark, or ids above the
append-only mark) are reported as LAG: the warehouse is behind, not wrong, and the next
`make load` fixes it. Anything else means data was lost or damaged: exit code 1.

Plus integrity checks on the warehouse itself (orphans, unresolvable AdvanceRef).
Every result is stored in meta.check_results, so the history is queryable.
"""
import sys
from datetime import datetime

from pipeline import config
from pipeline.arrow import to_arrow
from pipeline.tables import APPEND_ONLY, TABLES, Table

UTF8 = "Latin1_General_100_BIN2_UTF8"


def canon_sqlserver(col: str, typ: str) -> str:
    if typ == "TIMESTAMP":
        return f"CONVERT(VARCHAR(19), {col}, 120)"
    if typ == "DATE":
        return f"CONVERT(VARCHAR(10), {col}, 23)"
    if typ == "VARCHAR":
        return f"CONVERT(VARCHAR(MAX), {col} COLLATE {UTF8})"
    return f"CONVERT(VARCHAR(40), {col})"     # integers, decimals, bit -> '1'/'0'


def canon_duckdb(col: str, typ: str) -> str:
    if typ == "TIMESTAMP":
        return f"strftime({col}, '%Y-%m-%d %H:%M:%S')"
    if typ == "DATE":
        return f"strftime({col}, '%Y-%m-%d')"
    if typ == "BOOLEAN":
        return f"CASE WHEN {col} THEN '1' WHEN NOT {col} THEN '0' END"
    return f"CAST({col} AS VARCHAR)"


def fingerprint_sqlserver(t: Table) -> str:
    parts = ", '|', ".join(f"ISNULL({canon_sqlserver(c, typ)}, '~')" for c, typ in t.columns if c != t.pk)
    return f"LOWER(CONVERT(CHAR(32), HASHBYTES('MD5', CONCAT({parts})), 2))"


def fingerprint_duckdb(t: Table) -> str:
    parts = " || '|' || ".join(f"COALESCE({canon_duckdb(c, typ)}, '~')" for c, typ in t.columns if c != t.pk)
    return f"md5({parts})"


def compare(cur, wh, t: Table) -> dict:
    state = wh.execute("SELECT last_version, last_id FROM meta.load_state WHERE table_name = ?", [t.name]).fetchone()
    if state is None:
        return {"status": "FAIL", "note": "never loaded"}

    cur.execute(f"SELECT {t.pk} AS pk, {fingerprint_sqlserver(t)} AS h FROM dbo.{t.name}")
    wh.register("src", to_arrow(cur, cur.fetchall()))
    # Keys the loader has not been asked to bring yet: differences on these are lag, not errors.
    if t.strategy == APPEND_ONLY:
        cur.execute(f"SELECT {t.pk} AS pk FROM dbo.{t.name} WHERE {t.pk} > %s", (state[1],))
    else:
        cur.execute(f"SELECT {t.pk} AS pk FROM CHANGETABLE(CHANGES dbo.{t.name}, %s) AS ct", (state[0],))
    wh.register("pending", to_arrow(cur, cur.fetchall()))

    wh.execute(f"""CREATE OR REPLACE TEMP TABLE diff AS
        WITH w AS (SELECT {t.pk} AS pk, {fingerprint_duckdb(t)} AS h FROM raw.{t.name})
        SELECT COALESCE(s.pk, w.pk) AS pk,
               CASE WHEN w.pk IS NULL THEN 'missing' WHEN s.pk IS NULL THEN 'extra' ELSE 'mismatched' END AS kind,
               COALESCE(s.pk, w.pk) IN (SELECT pk FROM pending) AS explained
        FROM src s FULL OUTER JOIN w ON s.pk = w.pk
        WHERE s.pk IS NULL OR w.pk IS NULL OR s.h <> w.h""")
    counts = dict(wh.execute("SELECT kind || CASE WHEN explained THEN '_lag' ELSE '' END, COUNT(*) "
                             "FROM diff GROUP BY 1").fetchall())
    examples = wh.execute("SELECT kind, LIST(pk ORDER BY pk)[1:5] FROM diff WHERE NOT explained GROUP BY kind").fetchall()
    unexplained = sum(v for k, v in counts.items() if not k.endswith("_lag"))
    lag = sum(v for k, v in counts.items() if k.endswith("_lag"))
    rows_src = wh.execute("SELECT COUNT(*) FROM src").fetchone()[0]
    rows_wh = wh.execute(f"SELECT COUNT(*) FROM raw.{t.name}").fetchone()[0]
    wh.unregister("src")
    wh.unregister("pending")
    return {"status": "FAIL" if unexplained else "LAG" if lag else "PASS", "rows_source": rows_src,
            "rows_warehouse": rows_wh, "missing": counts.get("missing", 0), "extra": counts.get("extra", 0),
            "mismatched": counts.get("mismatched", 0), "lag": lag,
            "examples": {k: v for k, v in examples}, "bad_keys": wh.execute(
                "SELECT pk FROM diff WHERE NOT explained").fetchall()}


INTEGRITY = [
    ("advances without customer", "SELECT COUNT(*) FROM raw.Advances a WHERE a.CustomerId NOT IN (SELECT CustomerId FROM raw.Customers)"),
    ("cards without customer", "SELECT COUNT(*) FROM raw.Cards c WHERE c.CustomerId NOT IN (SELECT CustomerId FROM raw.Customers)"),
    ("transactions with unresolvable AdvanceRef", "SELECT COUNT(*) FROM clean.transactions t "
     "WHERE t.AdvanceId IS NULL OR t.AdvanceId NOT IN (SELECT AdvanceId FROM raw.Advances)"),
]


def run() -> int:
    wh = config.warehouse()
    wh.execute("""CREATE TABLE IF NOT EXISTS meta.check_results (
        checked_at TIMESTAMP, table_name VARCHAR, status VARCHAR, rows_source BIGINT, rows_warehouse BIGINT,
        missing BIGINT, extra BIGINT, mismatched BIGINT, lag BIGINT)""")
    wh.execute("CREATE TABLE IF NOT EXISTS meta.bad_keys (table_name VARCHAR, pk BIGINT)")
    wh.execute("DELETE FROM meta.bad_keys")
    now = datetime.now()

    with config.source(autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
        version = cur.fetchone()[0]
        results = {t.name: compare(cur, wh, t) for t in TABLES.values()}
        cur.execute("COMMIT")

    print(f"Source vs warehouse, checked at source CT version {version}\n")
    print(f"  {'table':<17}{'status':<7}{'source':>9}{'warehouse':>11}{'missing':>9}{'extra':>7}{'mismatch':>10}{'lag':>6}")
    for name, r in results.items():
        if r.get("note"):
            print(f"  {name:<17}{r['status']:<7}{r['note']}")
            continue
        print(f"  {name:<17}{r['status']:<7}{r['rows_source']:>9,}{r['rows_warehouse']:>11,}"
              f"{r['missing']:>9}{r['extra']:>7}{r['mismatched']:>10}{r['lag']:>6}")
        wh.execute("INSERT INTO meta.check_results VALUES (?,?,?,?,?,?,?,?,?)",
                   [now, name, r["status"], r["rows_source"], r["rows_warehouse"], r["missing"], r["extra"], r["mismatched"], r["lag"]])
        for (pk,) in r["bad_keys"]:
            wh.execute("INSERT INTO meta.bad_keys VALUES (?, ?)", [name, pk])

    print("\nIntegrity inside the warehouse:")
    integrity_failed = False
    for label, sql in INTEGRITY:
        n = wh.execute(sql).fetchone()[0]
        lag_any = any(r.get("lag") for r in results.values())
        status = "PASS" if n == 0 else "LAG" if lag_any else "FAIL"
        integrity_failed |= status == "FAIL"
        print(f"  {status:<6}{label}: {n}")
        wh.execute("INSERT INTO meta.check_results (checked_at, table_name, status, mismatched) VALUES (?,?,?,?)",
                   [now, f"integrity: {label}", status, n])

    failed = [n for n, r in results.items() if r["status"] == "FAIL"]
    lagging = [n for n, r in results.items() if r["status"] == "LAG"]
    print()
    for name in failed:
        for kind, keys in results[name].get("examples", {}).items():
            print(f"  {name}: {kind} keys (first 5): {keys}")
    if failed or integrity_failed:
        print("\nRESULT: FAIL - the warehouse lost or damaged data the loader did not account for.")
        print("        `make repair` re-copies exactly the flagged rows; investigate why before trusting reports.")
        wh.close()
        return 1
    if lagging:
        print(f"RESULT: LAG - no data is wrong, but {', '.join(lagging)} have source changes not loaded yet. Run `make load`.")
    else:
        print("RESULT: PASS - every row in the warehouse matches the source.")
    wh.close()
    return 0


def repair() -> None:
    """Re-copy only the keys flagged by the last check. Watermarks are not touched."""
    wh = config.warehouse()
    flagged = wh.execute("SELECT table_name, LIST(pk) FROM meta.bad_keys GROUP BY 1").fetchall()
    if not flagged:
        print("Nothing to repair: the last check flagged no rows.")
        return
    with config.source() as conn:
        cur = conn.cursor()
        for name, keys in flagged:
            t = TABLES[name]
            rows = []
            for i in range(0, len(keys), 1000):
                in_list = ", ".join(str(int(k)) for k in keys[i:i + 1000])
                cur.execute(f"SELECT {', '.join(t.names)} FROM dbo.{t.name} WHERE {t.pk} IN ({in_list})")
                rows += cur.fetchall()
            wh.register("fix", to_arrow(cur, rows))
            wh.register("keys", to_arrow(type("D", (), {"description": [("pk",)]})(), [(k,) for k in keys]))
            wh.begin()
            wh.execute(f"DELETE FROM raw.{t.name} WHERE {t.pk} IN (SELECT pk FROM keys)")
            if rows:
                version = wh.execute(f"SELECT MAX(_source_version) FROM raw.{t.name}").fetchone()[0]
                wh.execute(f"INSERT INTO raw.{t.name} SELECT *, ?, now() FROM fix", [version])
            wh.commit()
            wh.unregister("fix")
            wh.unregister("keys")
            print(f"  {name}: re-copied {len(rows)} rows, removed {len(keys) - len(rows)} rows the source no longer has")
    wh.execute("DELETE FROM meta.bad_keys")
    wh.close()
    print("Repair done. Run `make check` to confirm.")


if __name__ == "__main__":
    if sys.argv[1:] == ["repair"]:
        repair()
    else:
        sys.exit(run())
