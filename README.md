# Fundo take-home — Data Engineer

A small local version of Fundo's SQL Server → BigQuery pipeline that addresses the three problems
in the brief: **incremental loading**, **duplicate customers**, and **proof that the warehouse is correct**.

- Source: SQL Server 2022 (Docker), synthetic data only
- Warehouse: DuckDB (a local stand-in for BigQuery)
- Pipeline: Python 3.12 (`pymssql`, `duckdb`, `pyarrow`)

Read [`SOLUTION.md`](SOLUTION.md) first for the reasoning, the numbers and the trade-offs.

## Requirements

- Docker (Docker Desktop is fine) and `make`.
- **Apple Silicon only:** SQL Server ships only amd64 images, so enable *Settings → General → "Use Rosetta for
  x86_64/amd64 emulation on Apple Silicon"* in Docker Desktop. On Intel/AMD machines nothing is needed.

No Python, drivers or cloud credentials are needed on the host; everything runs in containers.

## Run it

```bash
make demo
```

That single command starts the containers (it runs `docker compose up -d --wait` for you) and then, from a fresh source:

1. seeds the source database with deterministic synthetic data,
2. measures one simulated day of change (the numbers behind every decision),
3. does the initial load, simulates a business day, runs an incremental load,
4. checks source vs warehouse,
5. plans and applies the duplicate-customer merge, reloads and checks again,
6. simulates one more day, where re-registrations of an existing person are now rejected, reloads and checks.

It takes a few minutes (SQL Server is slow under emulation on Apple Silicon). The data is seeded with a fixed random
seed, so row counts and findings should match the ones quoted in `SOLUTION.md`.

Then the two failure demos:

```bash
make crash-demo    # crash mid-load, show nothing was half-written, resume
make break-demo    # check passes, damage the warehouse, check fails, repair, check passes
```

Start over at any time with `make reset` (deletes the source volume and the warehouse file), then `make demo`.

## Commands

| Command | What it does | What you should see |
|---|---|---|
| `make up` | Start SQL Server and the pipeline container; waits until SQL Server answers queries | Both containers `Healthy` |
| `make ping` | Connectivity smoke test | `source OK ...` and `warehouse OK DuckDB v1.5.5` |
| `make seed` | Recreate the source DB with synthetic data (removes the warehouse file, which would be stale) | Rows per table and the list of **planted cases** (duplicates, test accounts, traps) |
| `make mutate [DAYS=n]` | Simulate *n* business days of inserts, updates and deletes (deterministic per day) | Per-table counts, e.g. `customers_updated_without_updatedat 7` |
| `make measure` | Snapshot every row's hash, simulate a day, diff | Change rate per table, updates invisible to `UpdatedAt`, age of changed transactions, full copy vs changed-rows MB |
| `make load` | Initial load the first time, incremental afterwards | One line per table: strategy, mode, versions, rows upserted/deleted, payload KB |
| `make status` | Watermarks and the last loads | `meta.load_state` and `meta.load_log` |
| `make check` | Is the warehouse complete and correct right now? Exit code 1 on a real problem | `PASS` / `LAG` / `FAIL` per table + integrity checks |
| `make break` | Damage the warehouse on purpose (one missing, one extra, some mismatched rows) | What was damaged |
| `make repair` | Re-copy only the rows the last check flagged | `re-copied N rows` |
| `make crash-demo` | Simulate a day, crash while applying `Transactions`, show state, re-run | `FAILED: SIMULATED CRASH`, Transactions watermark unchanged, then a clean catch-up |
| `make break-demo` | `check` → `break` → `check` (fails) → `repair` → `check` (passes) | See below |
| `make dedupe-plan` | Classify customers, pick survivors, build the review queue. **Read-only** | Counts and representative cases |
| `make dedupe-apply` | Execute the plan in the source in one transaction, log every moved row, add the unique TaxId index | `Applied N merges ... created unique index` |
| `make dedupe-undo` | Reverse every applied merge from the log and drop the index | `Undid N merges` |
| `make down` / `make reset` | Stop containers / stop and delete all local data | |

### Reproducing the failing check

```bash
make load          # make sure the warehouse is current
make break-demo
```

Step 3 of the demo prints `FAIL` for Customers (3 missing), Cards (1 extra), Transactions (5 mismatched) and
Advances (1 mismatched), plus integrity failures for the advances and cards orphaned by the missing customers,
and lists the exact keys. The make target ignores that expected non-zero exit, repairs, and the last check prints
`RESULT: PASS`.

## Layout

```
docker-compose.yml     SQL Server + pipeline container
Makefile               every entry point, with a one-line description
sql/001_source_schema.sql   source schema, including the deliberate bad choices
pipeline/
  tables.py            single registry: columns, types, keys and load strategy per table
  seed.py, mutate.py   deterministic synthetic source and daily change simulator
  profile.py           measurement (make measure)
  load.py              incremental loader (Change Tracking + append-only watermark)
  check.py             source-vs-warehouse checks and targeted repair
  break_it.py          damages the warehouse for the failing-check demo
  dedupe.py            identity resolution: plan / apply / undo
```

Warehouse schemas (DuckDB file `data/warehouse.duckdb`): `raw` (loaded tables), `clean` (views that fix
`AdvanceRef` and add `PersonId`), `meta` (watermarks, load log, check history), `dedupe` (plan and review queue).
