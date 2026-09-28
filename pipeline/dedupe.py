"""Resolve duplicate customers: plan (read-only) -> apply (reversible) -> undo.

    python -m pipeline.dedupe plan    # classify, decide survivors, write the plan; touches nothing
    python -m pipeline.dedupe apply   # execute the plan in the SOURCE, in one transaction, fully logged
    python -m pipeline.dedupe undo    # reverse every applied merge from the log

Identity rules (the core of the problem):

  PROVES identity   TaxId (SSN/ITIN) + the same DateOfBirth.
                    A government id is issued to one person. Same TaxId with a DIFFERENT
                    birth date is not a typo we can resolve by guessing: it is the classic
                    synthetic-identity signal, so the group is held for review.
  PROVES DIFFERENT  Two different valid TaxIds are two different people, no matter how many
                    other fields they share (couples share email/address, carriers recycle
                    phone numbers, namesakes share names).
  ONLY SUGGESTS     email, phone, name, address, card fingerprint. Used to build a review
                    queue when at least one side has no TaxId. Never used to merge.

Business rules:
  * A customer with a funded or paid-off advance is untouchable: if it is in a group it is
    the survivor, and it only ever gains records. Two untouchable customers in one group:
    we do not guess -> both stay active, flagged IdentityHold, sent to review.
  * Test data is excluded, not merged, and identified by email only (domain or a test-like
    local part) — never by name, so "Testerman" is safe. Anything matching the pattern that
    has real money (funded/paid-off advance or settled transactions) goes to review instead.
  * Malformed emails/phones are flagged and counted, never "fixed" and never used as match
    evidence: guessing a digit could route a collection call to a stranger.
  * Cards of a merged customer move to the survivor (it keeps its own default card).
"""
import sys
from datetime import datetime

from pipeline import config
from pipeline.arrow import to_arrow

TEST_DOMAINS = ("example.com", "mailinator.com", "test.com", "asdf.com")
TEST_LOCAL = r"^(test|qa|demo|sandbox|staging|loadtest)[0-9]*([+._-].*)?$"
EMAIL_OK = r"^[a-z0-9._%+-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$"
UNIQUE_INDEX = "UX_Customers_TaxId_Active"


def fetch_source(wh) -> None:
    """Pull what identity resolution needs, in one consistent snapshot, into temp tables."""
    with config.source(autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("SELECT CustomerId, FirstName, LastName, Email, Phone, DateOfBirth, TaxId, CreatedAt, "
                    "MergedIntoCustomerId FROM dbo.Customers")
        wh.register("src_c", to_arrow(cur, cur.fetchall()))
        cur.execute("SELECT c.CustomerId, "
                    "MAX(CASE WHEN a.Status IN ('funded', 'paid_off') THEN 1 ELSE 0 END) AS protected, "
                    "COUNT(a.AdvanceId) AS advances, "
                    "(SELECT COUNT(*) FROM dbo.Transactions t WHERE t.CustomerId = c.CustomerId AND t.Status = 'settled') AS settled_txns "
                    "FROM dbo.Customers c LEFT JOIN dbo.Advances a ON a.CustomerId = c.CustomerId GROUP BY c.CustomerId")
        wh.register("src_m", to_arrow(cur, cur.fetchall()))
        cur.execute("SELECT CustomerId, Fingerprint FROM dbo.Cards")
        wh.register("src_cards", to_arrow(cur, cur.fetchall()))
        cur.execute("COMMIT")
    wh.execute("CREATE OR REPLACE TEMP TABLE c AS SELECT * FROM src_c")
    wh.execute("CREATE OR REPLACE TEMP TABLE m AS SELECT * FROM src_m")
    wh.execute("CREATE OR REPLACE TEMP TABLE cards AS SELECT * FROM src_cards")
    for name in ("src_c", "src_m", "src_cards"):
        wh.unregister(name)


def plan() -> None:
    wh = config.warehouse()
    wh.execute("CREATE SCHEMA IF NOT EXISTS dedupe")
    fetch_source(wh)
    domains = ", ".join(f"'{d}'" for d in TEST_DOMAINS)

    # 1. Normalize and assess contact quality (flag, never fix).
    wh.execute(f"""CREATE OR REPLACE TABLE dedupe.customers AS
        WITH n AS (
            SELECT c.*, m.protected, m.advances, m.settled_txns,
                   lower(trim(Email)) AS email_norm,
                   regexp_replace(Phone, '[^0-9]', '', 'g') AS digits
            FROM c JOIN m USING (CustomerId))
        SELECT *,
               regexp_full_match(email_norm, '{EMAIL_OK}') AND NOT contains(email_norm, '..') AS email_valid,
               regexp_replace(split_part(email_norm, '@', 1), '\\+.*$', '') || '@' || split_part(email_norm, '@', 2) AS email_key,
               CASE WHEN length(digits) = 11 AND digits LIKE '1%' THEN substr(digits, 2) ELSE digits END AS phone10,
               regexp_full_match(coalesce(TaxId, ''), '^[0-9]{{3}}-[0-9]{{2}}-[0-9]{{4}}$') AS taxid_valid,
               split_part(email_norm, '@', 2) IN ({domains})
                 OR (regexp_full_match(split_part(email_norm, '@', 1), '{TEST_LOCAL}')) AS test_pattern
        FROM n WHERE MergedIntoCustomerId IS NULL""")
    wh.execute("""ALTER TABLE dedupe.customers ADD COLUMN phone_valid BOOLEAN""")
    wh.execute("""UPDATE dedupe.customers SET phone_valid =
        length(phone10) = 10 AND substr(phone10, 1, 1) BETWEEN '2' AND '9' AND phone10 <> '0000000000'""")

    # 2. Test accounts: excluded, not merged. Pattern + real money => human review.
    wh.execute("""CREATE OR REPLACE TABLE dedupe.test_accounts AS
        SELECT CustomerId, Email, FirstName, LastName,
               CASE WHEN protected = 1 OR settled_txns > 0 THEN 'review: matches test pattern but has real money'
                    ELSE 'exclude' END AS action
        FROM dedupe.customers WHERE test_pattern""")

    # 3. Proof groups: same valid TaxId among active, non-test customers.
    wh.execute("""CREATE OR REPLACE TABLE dedupe.plan AS
        WITH eligible AS (
            SELECT * FROM dedupe.customers
            WHERE taxid_valid AND CustomerId NOT IN (SELECT CustomerId FROM dedupe.test_accounts WHERE action = 'exclude')),
        g AS (
            SELECT TaxId, COUNT(*) AS members, COUNT(DISTINCT DateOfBirth) AS dobs, SUM(protected) AS protected_members
            FROM eligible GROUP BY TaxId HAVING COUNT(*) > 1),
        ranked AS (
            SELECT e.CustomerId, e.TaxId, e.protected, g.members, g.dobs, g.protected_members,
                   ROW_NUMBER() OVER (PARTITION BY e.TaxId ORDER BY e.protected DESC, e.CreatedAt, e.CustomerId) AS rnk,
                   ROW_NUMBER() OVER (PARTITION BY e.TaxId ORDER BY e.CreatedAt, e.CustomerId) AS age_rank
            FROM eligible e JOIN g USING (TaxId))
        SELECT CustomerId, TaxId,
               CASE WHEN dobs > 1 THEN CASE WHEN age_rank = 1 THEN 'hold_keep' ELSE 'hold' END
                    WHEN protected_members > 1 THEN CASE WHEN age_rank = 1 THEN 'hold_keep' ELSE 'hold' END
                    WHEN rnk = 1 THEN 'survivor' ELSE 'merge' END AS action,
               FIRST_VALUE(CustomerId) OVER (PARTITION BY TaxId ORDER BY rnk) AS survivor_id,
               CASE WHEN dobs > 1 THEN 'same TaxId, different birth dates: possible synthetic identity'
                    WHEN protected_members > 1 THEN 'two customers with funded/paid-off advances: not guessing'
                    WHEN rnk = 1 AND protected = 1 AND age_rank > 1 THEN 'survivor holds the funded/paid-off advance (not the oldest record)'
                    WHEN rnk = 1 AND protected = 1 THEN 'survivor holds the funded/paid-off advance'
                    WHEN rnk = 1 THEN 'oldest record'
                    ELSE 'duplicate of survivor (same TaxId and birth date)' END AS reason
        FROM ranked""")

    # 4. Suggestive matches -> review queue, only when at least one side lacks a TaxId.
    wh.execute("""CREATE OR REPLACE TABLE dedupe.review_queue AS
        WITH x AS (SELECT * FROM dedupe.customers
                   WHERE CustomerId NOT IN (SELECT CustomerId FROM dedupe.test_accounts WHERE action = 'exclude')),
        pairs AS (
            SELECT a.CustomerId AS a_id, b.CustomerId AS b_id, 'email' AS signal
            FROM x a JOIN x b ON a.email_key = b.email_key AND a.CustomerId < b.CustomerId WHERE a.email_valid AND b.email_valid
            UNION ALL
            SELECT a.CustomerId, b.CustomerId, 'phone'
            FROM x a JOIN x b ON a.phone10 = b.phone10 AND a.CustomerId < b.CustomerId WHERE a.phone_valid AND b.phone_valid
            UNION ALL
            SELECT a.CustomerId, b.CustomerId, 'name+birthdate'
            FROM x a JOIN x b ON lower(a.LastName) = lower(b.LastName) AND a.DateOfBirth = b.DateOfBirth
                               AND a.CustomerId < b.CustomerId
            UNION ALL
            SELECT a.CustomerId, b.CustomerId, 'card fingerprint'
            FROM cards a JOIN cards b ON a.Fingerprint = b.Fingerprint AND a.CustomerId < b.CustomerId)
        SELECT p.a_id, p.b_id, list_sort(list_distinct(list(signal))) AS signals,
               CASE WHEN count(DISTINCT signal) >= 2 THEN 'likely same person' ELSE 'possible match' END AS assessment
        FROM pairs p JOIN x a ON a.CustomerId = p.a_id JOIN x b ON b.CustomerId = p.b_id
        WHERE a.TaxId IS NULL OR b.TaxId IS NULL
        GROUP BY p.a_id, p.b_id""")
    lookalikes = wh.execute("""
        SELECT COUNT(*) FILTER (WHERE shared_contact), COUNT(*) FILTER (WHERE same_name)
        FROM (SELECT (a.email_valid AND b.email_valid AND a.email_key = b.email_key)
                     OR (a.phone_valid AND b.phone_valid AND a.phone10 = b.phone10) AS shared_contact,
                     lower(a.FirstName) = lower(b.FirstName) AND lower(a.LastName) = lower(b.LastName) AS same_name
              FROM dedupe.customers a JOIN dedupe.customers b
                ON a.CustomerId < b.CustomerId AND a.taxid_valid AND b.taxid_valid AND a.TaxId <> b.TaxId
               AND (a.email_key = b.email_key OR a.phone10 = b.phone10
                    OR (lower(a.FirstName) = lower(b.FirstName) AND lower(a.LastName) = lower(b.LastName))))""").fetchone()
    wh.execute("CREATE OR REPLACE TABLE dedupe.plan_meta AS SELECT now() AS planned_at")

    report(wh, lookalikes)
    wh.close()


def report(wh, lookalikes: tuple[int, int]) -> None:
    def q(sql: str) -> int:
        return wh.execute(sql).fetchone()[0]
    n = {
        "active": q("SELECT COUNT(*) FROM dedupe.customers"),
        "bad_email": q("SELECT COUNT(*) FROM dedupe.customers WHERE email_valid IS NOT TRUE"),
        "bad_phone": q("SELECT COUNT(*) FROM dedupe.customers WHERE phone_valid IS NOT TRUE"),
        "tests": q("SELECT COUNT(*) FROM dedupe.test_accounts WHERE action = 'exclude'"),
        "tests_money": q("SELECT COUNT(*) FROM dedupe.test_accounts WHERE action LIKE 'review%'"),
        "groups": q("SELECT COUNT(DISTINCT TaxId) FROM dedupe.plan"),
        "merges": q("SELECT COUNT(*) FROM dedupe.plan WHERE action = 'merge'"),
        "held": q("SELECT COUNT(DISTINCT TaxId) FROM dedupe.plan WHERE action LIKE 'hold%'"),
        "queue": q("SELECT COUNT(*) FROM dedupe.review_queue"),
        "likely": q("SELECT COUNT(*) FROM dedupe.review_queue WHERE assessment = 'likely same person'"),
    }
    print("Identity resolution plan (nothing has been changed yet)\n")
    print(f"  active customers analysed               {n['active']:>6,}")
    print(f"  malformed emails (flagged, not fixed)   {n['bad_email']:>6,}")
    print(f"  malformed phones (flagged, not fixed)   {n['bad_phone']:>6,}")
    print(f"  test accounts to exclude                {n['tests']:>6,}")
    print(f"  test-looking accounts with real money   {n['tests_money']:>6,}   -> review, not excluded")
    print(f"  proven duplicate groups (TaxId + DOB)   {n['groups']:>6,}")
    print(f"    records to merge into a survivor      {n['merges']:>6,}")
    print(f"    groups held for review                {n['held']:>6,}")
    print(f"  share email/phone, different TaxIds     {lookalikes[0]:>6,}   -> couples, recycled numbers: different people, never merged")
    print(f"  same full name, different TaxIds        {lookalikes[1]:>6,}   -> why a name can only ever suggest a match")
    print(f"  review queue (no TaxId on one side)     {n['queue']:>6,}   ({n['likely']} likely, rest possible)")

    print("\nCases worth a look (first 5 of each; everything is in dedupe.plan):")
    for label, cond in [("held for review", "bool_or(action LIKE 'hold%')"),
                        ("survivor is NOT the oldest record", "bool_or(reason LIKE '%not the oldest%')")]:
        rows = wh.execute(f"""SELECT TaxId, list(CustomerId || ':' || action ORDER BY CustomerId),
                                     max(CASE WHEN action IN ('survivor', 'hold_keep') THEN reason END)
                              FROM dedupe.plan GROUP BY TaxId HAVING {cond} ORDER BY min(CustomerId)""").fetchall()
        print(f"  {label}: {len(rows)} groups")
        for taxid, members, reason in rows[:5]:
            print(f"    {members}  <- {reason}")
    print("\n  Test-pattern accounts kept for review:")
    kept = wh.execute("SELECT CustomerId, Email FROM dedupe.test_accounts WHERE action LIKE 'review%'").fetchall()
    for row in kept:
        print(f"    {row[0]}: {row[1]}")
    if not kept:
        print("    (none in this data set)")
    safe = wh.execute("SELECT list(FirstName || ' ' || LastName) FROM dedupe.customers "
                      "WHERE (LastName ILIKE '%test%' OR FirstName ILIKE '%test%') AND NOT test_pattern").fetchone()[0]
    print(f"  Names containing 'test' that are NOT treated as test data: {safe}")
    print("\nPlan stored in the warehouse: dedupe.plan, dedupe.test_accounts, dedupe.review_queue.")
    print("Next: `make dedupe-apply` (reversible with `make dedupe-undo`).")


def apply() -> None:
    wh = config.warehouse()
    merges = wh.execute("SELECT CustomerId, survivor_id FROM dedupe.plan WHERE action = 'merge' ORDER BY CustomerId").fetchall()
    holds = wh.execute("SELECT CustomerId, reason FROM dedupe.plan WHERE action = 'hold'").fetchall()
    wh.close()
    now = datetime.now().replace(microsecond=0)
    applied = skipped = 0
    with config.source() as conn:
        cur = conn.cursor()
        for merged, survivor in merges:
            # Re-validate against the source at apply time: never merge away a protected customer.
            cur.execute("SELECT COUNT(*) FROM dbo.Advances WHERE CustomerId = %s AND Status IN ('funded', 'paid_off')", (merged,))
            if cur.fetchone()[0] > 0:
                print(f"  skip {merged}: it now holds a funded/paid-off advance (plan is stale)")
                skipped += 1
                continue
            cur.execute("SELECT COUNT(*) FROM dbo.Customers WHERE CustomerId = %s AND MergedIntoCustomerId IS NULL", (survivor,))
            if cur.fetchone()[0] == 0:
                print(f"  skip {merged}: survivor {survivor} no longer active (plan is stale)")
                skipped += 1
                continue
            log = ("INSERT INTO dbo.CustomerMerges (MergedCustomerId, SurvivorCustomerId, EntityTable, EntityId, "
                   "PreviousIsDefault, MergedAt) ")
            cur.execute(log + "SELECT %s, %s, 'Advances', AdvanceId, NULL, %s FROM dbo.Advances WHERE CustomerId = %s",
                        (merged, survivor, now, merged))
            cur.execute(log + "SELECT %s, %s, 'Cards', CardId, IsDefault, %s FROM dbo.Cards WHERE CustomerId = %s",
                        (merged, survivor, now, merged))
            cur.execute(log + "SELECT %s, %s, 'Transactions', TransactionId, NULL, %s FROM dbo.Transactions WHERE CustomerId = %s",
                        (merged, survivor, now, merged))
            cur.execute(log + "VALUES (%s, %s, 'Customers', %s, NULL, %s)", (merged, survivor, merged, now))
            cur.execute("UPDATE dbo.Advances SET CustomerId = %s, UpdatedAt = %s WHERE CustomerId = %s", (survivor, now, merged))
            cur.execute("UPDATE dbo.Cards SET CustomerId = %s, UpdatedAt = %s, "
                        "IsDefault = CASE WHEN EXISTS (SELECT 1 FROM dbo.Cards d WHERE d.CustomerId = %s AND d.IsDefault = 1) "
                        "THEN 0 ELSE IsDefault END WHERE CustomerId = %s", (survivor, now, survivor, merged))
            cur.execute("UPDATE dbo.Transactions SET CustomerId = %s WHERE CustomerId = %s", (survivor, merged))
            cur.execute("UPDATE dbo.Customers SET MergedIntoCustomerId = %s, UpdatedAt = %s WHERE CustomerId = %s",
                        (survivor, now, merged))
            applied += 1
        for cid, reason in holds:
            code = "taxid_dob_mismatch" if "birth" in reason else "two_protected_customers"
            cur.execute("UPDATE dbo.Customers SET IdentityHold = %s, UpdatedAt = %s WHERE CustomerId = %s", (code, now, cid))
        # Prevention: one ACTIVE, non-held customer per TaxId, enforced by the database itself.
        cur.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{UNIQUE_INDEX}') "
                    f"CREATE UNIQUE INDEX {UNIQUE_INDEX} ON dbo.Customers (TaxId) "
                    f"WHERE TaxId IS NOT NULL AND MergedIntoCustomerId IS NULL AND IdentityHold IS NULL")
        conn.commit()
    print(f"Applied {applied} merges ({skipped} skipped), flagged {len(holds)} held records, "
          f"created unique index {UNIQUE_INDEX} on active TaxIds.")
    print("Every change is logged in dbo.CustomerMerges. Run `make load` then `make check` to propagate and verify.")


def undo() -> None:
    now = datetime.now().replace(microsecond=0)
    with config.source() as conn:
        cur = conn.cursor()
        cur.execute(f"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{UNIQUE_INDEX}') DROP INDEX {UNIQUE_INDEX} ON dbo.Customers")
        cur.execute("UPDATE a SET CustomerId = l.MergedCustomerId, UpdatedAt = %s FROM dbo.Advances a "
                    "JOIN dbo.CustomerMerges l ON l.EntityTable = 'Advances' AND l.EntityId = a.AdvanceId AND l.UndoneAt IS NULL", (now,))
        cur.execute("UPDATE c SET CustomerId = l.MergedCustomerId, IsDefault = l.PreviousIsDefault, UpdatedAt = %s FROM dbo.Cards c "
                    "JOIN dbo.CustomerMerges l ON l.EntityTable = 'Cards' AND l.EntityId = c.CardId AND l.UndoneAt IS NULL", (now,))
        cur.execute("UPDATE t SET CustomerId = l.MergedCustomerId FROM dbo.Transactions t "
                    "JOIN dbo.CustomerMerges l ON l.EntityTable = 'Transactions' AND l.EntityId = t.TransactionId AND l.UndoneAt IS NULL")
        cur.execute("UPDATE c SET MergedIntoCustomerId = NULL, UpdatedAt = %s FROM dbo.Customers c "
                    "JOIN dbo.CustomerMerges l ON l.EntityTable = 'Customers' AND l.EntityId = c.CustomerId AND l.UndoneAt IS NULL", (now,))
        n = cur.rowcount
        cur.execute("UPDATE dbo.Customers SET IdentityHold = NULL, UpdatedAt = %s WHERE IdentityHold IS NOT NULL", (now,))
        cur.execute("UPDATE dbo.CustomerMerges SET UndoneAt = %s WHERE UndoneAt IS NULL", (now,))
        conn.commit()
    print(f"Undid {n} merges, cleared review holds and dropped {UNIQUE_INDEX}. Run `make load` then `make check`.")


if __name__ == "__main__":
    {"plan": plan, "apply": apply, "undo": undo}[sys.argv[1]]()
