"""Measure before deciding: table sizes, real change rates and the cost of a full copy.

    python -m pipeline.profile baseline   # snapshot every row's hash
    python -m pipeline.mutate             # one simulated business day
    python -m pipeline.profile report     # diff against the snapshot and print the numbers

`make measure` runs the three in order. The snapshot is a SHA-256 per row computed
inside SQL Server, so the diff sees every change — including the ones a timestamp
would miss. Results are also written to data/measurements.json for SOLUTION.md.
"""
import json
import sys

import duckdb

from pipeline import config
from pipeline.arrow import to_arrow
from pipeline.tables import EXCLUDED, TABLES, row_hash_sql

PROFILE_DB = config.ROOT / "data" / "profile.duckdb"
OUT = config.ROOT / "data" / "measurements.json"


def sizes(cur) -> dict[str, dict]:
    """Rows and data size (clustered index or heap only) per table, from SQL Server's own stats."""
    out = {}
    for name in list(TABLES) + list(EXCLUDED):
        cur.execute("SELECT SUM(row_count), SUM(used_page_count) * 8 / 1024.0 FROM sys.dm_db_partition_stats "
                    "WHERE object_id = OBJECT_ID(%s) AND index_id IN (0, 1)", (f"dbo.{name}",))
        rows, mb = cur.fetchone()
        out[name] = {"rows": int(rows), "mb": float(mb), "avg_row_bytes": float(mb) * 1024 * 1024 / max(int(rows), 1)}
    return out


def snapshot(cur, prof: duckdb.DuckDBPyConnection, prefix: str) -> None:
    """Store (pk, row hash, timestamps) for every row of every loaded table."""
    for t in TABLES.values():
        extras = [c for c in dict.fromkeys([t.updated_at, t.created_at]) if c]
        cur.execute(f"SELECT {t.pk} AS pk, {row_hash_sql(t)} AS h"
                    + "".join(f", {c}" for c in extras) + f" FROM dbo.{t.name}")
        prof.register("batch", to_arrow(cur, cur.fetchall()))
        prof.execute(f"CREATE OR REPLACE TABLE {prefix}_{t.name} AS SELECT * FROM batch")
        prof.unregister("batch")


def sim_day(cur) -> tuple[int, str]:
    cur.execute("SELECT Day, SimDate FROM sim.Clock")
    day, d = cur.fetchone()
    return day, str(d)


def baseline() -> None:
    with config.source() as conn:
        cur = conn.cursor()
        prof = duckdb.connect(str(PROFILE_DB))
        snapshot(cur, prof, "before")
        day, d = sim_day(cur)
        prof.execute("CREATE OR REPLACE TABLE meta AS SELECT ? AS day, ? AS sim_date", [day, d])
        prof.close()
    print(f"Baseline snapshot taken at simulated day {day} ({d}).")


def report() -> None:
    prof = duckdb.connect(str(PROFILE_DB))
    with config.source() as conn:
        cur = conn.cursor()
        snapshot(cur, prof, "after")
        size = sizes(cur)
        day_after, date_after = sim_day(cur)
        # Bad schema choice: AdvanceRef is text. How many rows does a naive join lose?
        cur.execute("SELECT COUNT(*), "
                    "SUM(CASE WHEN a.AdvanceId IS NULL THEN 1 ELSE 0 END) "
                    "FROM dbo.Transactions t LEFT JOIN dbo.Advances a "
                    "ON CAST(a.AdvanceId AS NVARCHAR(20)) = t.AdvanceRef")
        total_txn, naive_miss = cur.fetchone()
        cur.execute("SELECT SUM(CASE WHEN a.AdvanceId IS NULL THEN 1 ELSE 0 END) "
                    "FROM dbo.Transactions t LEFT JOIN dbo.Advances a "
                    "ON a.AdvanceId = TRY_CAST(LTRIM(RTRIM(t.AdvanceRef)) AS INT)")
        normalized_miss = cur.fetchone()[0]
    day_before = prof.execute("SELECT day FROM meta").fetchone()[0]

    results: dict = {"measured_between_days": [day_before, day_after], "sim_date": date_after, "tables": {}}
    print(f"\nChanges between simulated day {day_before} and day {day_after}  (MEASURED)\n")
    print(f"  {'table':<18}{'rows':>9}{'MB':>8}{'ins':>7}{'upd':>7}{'del':>6}{'changed %':>11}")
    full_mb = incr_mb = 0.0
    for t in TABLES.values():
        b, a = f"before_{t.name}", f"after_{t.name}"
        ins, dele, upd = prof.execute(f"""
            SELECT (SELECT COUNT(*) FROM {a} WHERE pk NOT IN (SELECT pk FROM {b})),
                   (SELECT COUNT(*) FROM {b} WHERE pk NOT IN (SELECT pk FROM {a})),
                   (SELECT COUNT(*) FROM {a} JOIN {b} USING (pk) WHERE {a}.h <> {b}.h)""").fetchone()
        s = size[t.name]
        changed = ins + upd + dele
        pct = 100.0 * changed / max(s["rows"], 1)
        full_mb += s["mb"]
        incr_mb += changed * s["avg_row_bytes"] / 1024 / 1024
        info = {"rows": s["rows"], "mb": round(s["mb"], 2), "avg_row_bytes": round(s["avg_row_bytes"]),
                "inserts": ins, "updates": upd, "deletes": dele, "changed_pct": round(pct, 2)}

        if t.updated_at:   # would a "WHERE UpdatedAt > last_run" loader have seen every update?
            missed = prof.execute(f"SELECT COUNT(*) FROM {a} JOIN {b} USING (pk) "
                                  f"WHERE {a}.h <> {b}.h AND {a}.{t.updated_at} IS NOT DISTINCT FROM {b}.{t.updated_at}").fetchone()[0]
            info["updates_invisible_to_updatedat"] = missed
        if t.name == "Transactions":   # how old are the rows that change?
            ages = prof.execute(f"""
                SELECT CASE WHEN age <= 3 THEN '0-3 days' WHEN age <= 7 THEN '4-7 days'
                            WHEN age <= 30 THEN '8-30 days' ELSE '>30 days' END AS bucket, COUNT(*)
                FROM (SELECT date_diff('day', CAST({a}.CreatedAt AS DATE), DATE '{date_after}') AS age
                      FROM {a} JOIN {b} USING (pk) WHERE {a}.h <> {b}.h)
                GROUP BY 1 ORDER BY MIN(age)""").fetchall()
            info["updated_rows_by_age"] = dict(ages)
        results["tables"][t.name] = info
        print(f"  {t.name:<18}{s['rows']:>9,}{s['mb']:>8.2f}{ins:>7}{upd:>7}{dele:>6}{pct:>10.2f}%")

    for name in EXCLUDED:
        s = size[name]
        full_mb += s["mb"]   # today's daily job copies the whole database, scratch tables included
        results["tables"][name] = {"rows": s["rows"], "mb": round(s["mb"], 2), "loaded": False}
        print(f"  {name:<18}{s['rows']:>9,}{s['mb']:>8.2f}   (scratch table, never changes, not loaded)")

    print("\nFindings (MEASURED):")
    for name, info in results["tables"].items():
        if "updates_invisible_to_updatedat" in info:
            print(f"  {name}: {info['updates_invisible_to_updatedat']} of {info['updates']} updated rows kept the same UpdatedAt")
    print(f"  Transactions: updated rows by age -> {results['tables']['Transactions']['updated_rows_by_age']}")
    print(f"  Transactions.AdvanceRef: naive text join loses {naive_miss} of {total_txn:,} rows; "
          f"trim+cast join loses {normalized_miss}")

    ratio = full_mb / incr_mb if incr_mb else float("inf")
    results["transfer"] = {"full_copy_mb_per_day": round(full_mb, 2), "changed_rows_mb_per_day": round(incr_mb, 3),
                           "ratio": round(ratio, 1)}
    results["advance_ref"] = {"rows": total_txn, "naive_join_misses": naive_miss, "normalized_join_misses": normalized_miss}
    print(f"\nTransfer per day (MEASURED sizes; changed-row volume ESTIMATED as rows x average row size):")
    print(f"  full copy (today)          : {full_mb:8.2f} MB")
    print(f"  only changed rows          : {incr_mb:8.3f} MB   (~{ratio:.0f}x less)")

    OUT.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nSaved to {OUT.relative_to(config.ROOT)}")
    prof.close()


if __name__ == "__main__":
    {"baseline": baseline, "report": report}[sys.argv[1]]()
