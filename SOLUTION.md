# Solution

I took on all three problems. The order I would ship them in is not the order of the brief (see the end).
Every number below is labelled **measured** (printed by a command in this repo, from the deterministic seed) or
**estimated**.

## 0. Measure first

Before choosing any strategy I built `make measure`: it stores a SHA-256 of every source row, simulates one
business day, and diffs. One day, **measured**:

| Table | Rows | Inserts | Updates | Deletes | Changed |
|---|---|---|---|---|---|
| Customers | 6,295 | 12 | 40 | 2 | 0.86% |
| Advances | 6,629 | 15 | 32 | 0 | 0.71% |
| Transactions | 77,015 | 248 | 179 | 1 | 0.56% |
| Cards | 5,652 | 8 | 4 | 3 | 0.27% |
| CustomerHistory | 6,282 | 40 | 0 | 0 | 0.64% |

Three findings changed my plan:

1. **`UpdatedAt` cannot be trusted.** 7 of 40 updated customers kept the same `UpdatedAt` (a support script
   forgets it). A `WHERE UpdatedAt > last_run` loader would miss them silently. And no timestamp ever shows a delete.
2. **"Transactions rarely change" is only true for old rows.** My first plan was append-only by id for the big
   table. The data disagreed: 179 transactions changed in one day, **all 4–7 days old** (pending payments settling,
   bounced payments reversed). Append-only would leave them `pending` in the warehouse forever. I changed strategy.
3. **The text identifier costs real rows.** Joining `Transactions.AdvanceRef` (NVARCHAR(MAX)) naively loses
   **2,319 of 77,015 rows (3.0%)** because of padding and leading zeros; trim + cast loses 0.

The simulator also showed the difference between replicating *operations* and replicating *state*: a payment
inserted and voided on the same day nets to nothing, so the warehouse only needs net changes per key.

## 1. Move only what changed

| Table | Strategy | Why |
|---|---|---|
| Customers, Advances, Cards | SQL Server **Change Tracking** + delete/insert by key | Updates without `UpdatedAt`; deletes |
| Transactions | **Change Tracking** | Young rows change (finding 2) |
| CustomerHistory | **Append-only high-water mark** on `HistoryId` | Append-only by design; 0 updates/deletes measured, so CT overhead buys nothing |
| tmp_marketing_export_2023 | **Not loaded** | Never changes, no owner. Recommend rename, wait 30 days, then drop |

I rejected "append-only + re-read the last 7 days" for Transactions: it works on this data, but a late chargeback
or reversal at day 30 would be lost silently. Change Tracking has no such window and costs little at <1% churn.

How it stays correct:

- **Consistent extract.** All tables are read in one snapshot-isolation transaction together with
  `CHANGE_TRACKING_CURRENT_VERSION()`, so the rows and the version describe the same instant.
- **Deletes.** For each changed key we left-join the live table; a key with no row is a delete.
- **Atomic apply, idempotent replay.** Per table, the row changes and the new watermark are committed in one DuckDB
  transaction, applied as delete-then-insert by primary key. Running twice gives the same result (**measured**: a
  second `make load` with no source changes moved 0 rows). `make crash-demo` crashes halfway through Transactions: the tables before it commit and
  advance, Transactions keeps its old watermark with nothing half-written, and the re-run brings exactly its
  pending changes; `make check` passes afterwards.
- **Retention.** If a watermark is older than CT retention (7 days), that table is fully resynced instead of
  silently missing changes.
- **Out-of-order identity commits.** The append-only load re-reads 100 ids below the mark. I first used 500;
  **measured** it was a third of the whole incremental payload, so I reduced it.

**Transfer, measured** (Arrow payload moved by the loader): initial load **7,810 KB**, a normal incremental day
**73 KB**, about **107× less**. The estimate from `make measure` (changed rows × average row size, against a full
copy
that also includes the scratch table) was ~169×. The gap is mostly the 100-id overlap re-read.

## 2. Resolve duplicate customers

**Identity rules.** This is the part that decides whether we merge two strangers.

- **Proves same person:** TaxId (SSN/ITIN) **and** the same date of birth. Same TaxId with a *different* birth
  date is not a typo I can fix by guessing. It is the classic synthetic-identity signal, so that group is held for
  review. (None in this data, but the rule is in the code.)
- **Proves different people:** two different valid TaxIds, whatever else they share. **Measured**: 23 pairs share an
  email or phone but have different TaxIds (couples, recycled numbers), and 8,732 pairs share a full name. None are
  merged.
- **Only suggests:** email (normalised, `+alias` stripped), phone (digits only), name + birth date, card
  fingerprint. They never merge anything. When at least one side has no TaxId, matches go to a **review queue**
  (**measured**: 67 pairs, all of them true duplicates planted by the generator).

**Business rules**

- **Funded / paid-off customers are untouchable.** Such a customer is always the survivor, even when it is not the
  oldest record (23 groups). It only gains rows. When **two** protected customers share an identity (25 groups),
  I do not guess: both stay active with `IdentityHold` set. I would send them to collections/ops, because merging
  would combine two contracts' ledgers. Meanwhile `clean.customers.PersonId` already groups them, so analytics
  counts people correctly. The share of held groups is high because of how the synthetic data was generated; I
  would measure it on real data before sizing the review work.
- **Test data is excluded, not merged,** and identified **by email only**: test domains, or a local part like
  `test`, `qa+1`, `demo`, `loadtest`. Never by name. **Measured**: exactly the 14 planted test accounts are excluded;
  Testerman ×3, Celeste Testa and four staff members with `@fundo.com` addresses and real advances are kept. Anything
  matching the pattern that has a funded advance or settled transactions goes to review instead of being excluded.
- **Malformed contacts** (148 emails, 140 phones, **measured**) are flagged and counted, never fixed, and never used
  as match evidence. Guessing a digit can route a collections call to a stranger. The real fix is validation at
  signup.
- **Cards** of a merged customer move to the survivor, which keeps its own default card. Getting this wrong means
  charging the wrong person, or two "default" cards and an ambiguous debit.

**Safety.** `dedupe plan` is read-only and writes the plan to the warehouse. `dedupe apply` runs in one source
transaction:
- It re-validates each merge at apply time and skips any record that became protected since the plan.
- It logs every re-pointed row in `dbo.CustomerMerges`, so `dedupe undo` reverses everything.

Because the merge happens in the source, Change Tracking carries it to the warehouse and `make check` passes
afterwards (**measured**: 154 merges applied, check PASS).

**Prevention.** A filtered unique index on `TaxId` for active, non-held customers. **Measured**: the next simulated
day, 2 re-registration attempts were rejected by the database.

## 3. Prove the data is correct

`make check` compares every key on both sides using an MD5 of a canonical text form of each row. Dates, decimals,
booleans and UTF-8 are rendered identically in T-SQL and DuckDB, so the hashes match. It reports three kinds of
difference:
- **missing:** a gap in the warehouse.
- **extra:** a delete that never arrived.
- **mismatched:** a stale or corrupted row.

It then asks Change Tracking which keys changed since the last load:
- Differences on those keys are **LAG**: the warehouse is behind, not wrong, and `make load` fixes it.
- Anything else is **FAIL**, with exit code 1.

Without this distinction a check fails whenever the job runs late, and people learn to ignore it.

It also checks integrity inside the warehouse (orphans, unresolvable `AdvanceRef`) and stores every result in
`meta.check_results`.

**Measured:** during `make demo` all 102,210 rows matched on the first run. `make break-demo` deletes 3 customers,
inserts a phantom card and alters 6 rows. The check reports exactly those keys, plus the advances and cards those
missing customers left orphaned. `make repair` re-copies only
those rows, and the check passes again.

## Cost impact (estimated)

What drives the bill today:
- Copying the whole database every day: cross-cloud egress from Azure, load jobs, and a daily full read on the
  production SQL Server.
- Downstream queries over tables that are rebuilt every day.

With change-based loads, egress and source reads scale with the ~0.3–0.9% of rows that change (measured locally),
not with the database size. The new cost to manage is the `MERGE` in BigQuery: it scans the target table, so large
tables should be partitioned and clustered on the merge key or date. Absolute savings depend on the real database
size, which I do not have.

## Deliberately skipped

- **Bucketed checksums.** At this size a full key+hash comparison is simplest. At production size I would compare
  per-bucket aggregates first and only drill into mismatched buckets, and run it off-peak or on a replica.
- **Fuzzy name matching.** Names only ever *suggest* a match, so better fuzziness would only grow the review queue.
- **Schema drift detection.** A new source column needs a one-line change in `tables.py`. In production I would
  alert on drift.
- **A review UI**, and loading `dbo.CustomerMerges` to the warehouse.
- **Real concurrency.** The out-of-order identity case is handled by design, but the simulator does not generate
  concurrent writers.

## Towards production

**Tools.** Keep SQL Server Change Tracking as the change signal; it is cheap and already proven here. For the
movement, I would first evaluate a managed CDC service into BigQuery (for example Datastream, which supports SQL
Server sources) against Fundo's exact SQL Server/Azure setup. If it fits, it removes most of the loader code. If
not, this loader runs as a small scheduled container writing to BigQuery staging tables followed by a `MERGE`. The
checks become a scheduled job with alerting, and the integrity rules become dbt tests.

**One-time vs permanent.**
- **One-time:** enabling Change Tracking, the initial load, the historical dedupe (plan → review → apply), retiring
  the scratch table, and migrating `AdvanceRef` to an integer foreign key.
- **Permanent:** incremental loads, the source-vs-warehouse check, the unique TaxId index, duplicate matching at
  signup feeding the review queue, and contact validation at input.

**What I would deliver first: the check.** Trust is the problem that hurts decisions today, and the check works
against the *current* full-copy pipeline with no other change. It tells Fundo where the warehouse is wrong right
now. It then becomes the safety net for the migration: run full-copy and incremental side by side, and switch over
once the check has passed for a couple of weeks.
