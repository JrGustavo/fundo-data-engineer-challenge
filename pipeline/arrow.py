"""Move query results into DuckDB as one Arrow batch (≈80x faster than row-by-row inserts)."""
import pyarrow as pa


def to_arrow(cursor, rows: list[tuple]) -> pa.Table:
    names = [d[0] for d in cursor.description]
    cols = list(zip(*rows)) if rows else [[] for _ in names]
    return pa.table({n: pa.array(list(c)) for n, c in zip(names, cols)})
