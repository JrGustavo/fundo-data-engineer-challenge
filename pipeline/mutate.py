"""Simulate one business day of changes in the source database.

Each run advances sim.Clock by one day and applies a realistic, deterministic mix of
inserts, updates and deletes (day N always produces the same changes), so the
incremental loader has something real to catch:

  * Customers: sign-ups (some are re-registrations of an existing person), contact
    updates logged to CustomerHistory, account closures (DELETE). A support script
    updates some rows WITHOUT bumping UpdatedAt, like it does in real life.
  * Advances: new applications and status transitions (approved -> funded -> paid_off).
  * Transactions: new repayments/disbursements, pending payments settling a few days
    later (UPDATE), an occasional bounced payment (UPDATE + fee) and a voided row (DELETE).
  * Cards: new cards, card renewals (UPDATE), removed cards (DELETE).
  * The scratch table is never touched.
"""
import random
from datetime import datetime, time, timedelta
from decimal import Decimal

from pipeline import config
from pipeline.seed import SEED, Generator


def ids(cur, sql: str) -> list:
    cur.execute(sql)
    return [r[0] for r in cur.fetchall()]


def main() -> None:
    with config.source() as conn:
        cur = conn.cursor()
        cur.execute("SELECT Day, SimDate FROM sim.Clock")
        day, simdate = cur.fetchone()
        day += 1
        simdate = simdate + timedelta(days=1)
        rng = random.Random(SEED * 1000 + day)
        gen = Generator()
        gen.rng = rng

        def ts() -> datetime:
            return datetime.combine(simdate, time(8)) + timedelta(seconds=rng.randint(0, 10 * 3600))

        stats: dict[str, int] = {}

        def count(key: str, n: int = 1) -> None:
            stats[key] = stats.get(key, 0) + n

        # ---- Customers: sign-ups, some of them re-registrations of an existing person
        with_taxid = ids(cur, "SELECT CustomerId FROM dbo.Customers WHERE TaxId IS NOT NULL ORDER BY CustomerId")
        for i in range(12):
            if i < 2:
                cur.execute("SELECT FirstName, LastName, Email, Phone, DateOfBirth, TaxId, AddressLine, City, State, ZipCode "
                            "FROM dbo.Customers WHERE CustomerId = %s", (rng.choice(with_taxid),))
                f = cur.fetchone()
                user, _, dom = (f[2] or "x@gmail.com").partition("@")
                row = (f[0], f[1], f"{user}+new@{dom}", f[3], f[4], f[5], f[6], f[7], f[8], f[9])
                count("customers_reregistered")
            else:
                p = gen.person(ts())
                row = tuple(p[c] for c in ["FirstName", "LastName", "Email", "Phone", "DateOfBirth", "TaxId",
                                           "AddressLine", "City", "State", "ZipCode"])
            now = ts()
            cur.execute("INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, DateOfBirth, TaxId, AddressLine, "
                        "City, State, ZipCode, CreatedAt, UpdatedAt) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        row + (now, now))
            count("customers_inserted")

        # ---- Customers: contact updates (history row always written; UpdatedAt sometimes forgotten)
        all_customers = ids(cur, "SELECT CustomerId FROM dbo.Customers ORDER BY CustomerId")
        for cid in rng.sample(all_customers, 40):
            field = rng.choice(["Email", "Phone", "AddressLine"])
            new = gen.person(ts())[field]
            cur.execute(f"SELECT {field} FROM dbo.Customers WHERE CustomerId = %s", (cid,))
            old = cur.fetchone()[0]
            when = ts()
            if rng.random() < 0.25:   # support script: changes the row but not UpdatedAt
                cur.execute(f"UPDATE dbo.Customers SET {field} = %s WHERE CustomerId = %s", (new, cid))
                count("customers_updated_without_updatedat")
            else:
                cur.execute(f"UPDATE dbo.Customers SET {field} = %s, UpdatedAt = %s WHERE CustomerId = %s", (new, when, cid))
            cur.execute("INSERT INTO dbo.CustomerHistory (CustomerId, FieldName, OldValue, NewValue, ChangedAt) "
                        "VALUES (%s,%s,%s,%s,%s)", (cid, field, old, new, when))
            count("customers_updated")
            count("history_inserted")

        # ---- Customers: account closures (only customers with no advances can be deleted)
        closable = ids(cur, "SELECT c.CustomerId FROM dbo.Customers c WHERE NOT EXISTS "
                            "(SELECT 1 FROM dbo.Advances a WHERE a.CustomerId = c.CustomerId) "
                            "AND c.Email NOT LIKE '%%@fundo.com' ORDER BY c.CustomerId")
        for cid in rng.sample(closable, 2):
            cur.execute("DELETE FROM dbo.Cards WHERE CustomerId = %s", (cid,))
            count("cards_deleted", cur.rowcount)
            cur.execute("DELETE FROM dbo.Customers WHERE CustomerId = %s", (cid,))
            count("customers_deleted")

        # ---- Advances: new applications and status transitions
        all_customers = ids(cur, "SELECT CustomerId FROM dbo.Customers ORDER BY CustomerId")
        for cid in rng.sample(all_customers, 15):
            now = ts()
            cur.execute("INSERT INTO dbo.Advances (CustomerId, Amount, FactorRate, Status, CreatedAt, UpdatedAt) "
                        "VALUES (%s,%s,%s,'pending',%s,%s)",
                        (cid, Decimal(rng.choice([500, 1000, 2000, 3000, 5000])), Decimal(str(round(rng.uniform(1.15, 1.49), 2))), now, now))
            count("advances_inserted")

        pending = ids(cur, "SELECT AdvanceId FROM dbo.Advances WHERE Status = 'pending' ORDER BY AdvanceId")
        for aid in rng.sample(pending, min(10, len(pending))):
            new = "approved" if rng.random() < 0.6 else "declined"
            cur.execute("UPDATE dbo.Advances SET Status = %s, UpdatedAt = %s WHERE AdvanceId = %s", (new, ts(), aid))
            count("advances_updated")

        approved = ids(cur, "SELECT AdvanceId FROM dbo.Advances WHERE Status = 'approved' ORDER BY AdvanceId")
        for aid in rng.sample(approved, min(8, len(approved))):
            when = ts()
            cur.execute("UPDATE dbo.Advances SET Status = 'funded', FundedAt = %s, UpdatedAt = %s WHERE AdvanceId = %s", (when, when, aid))
            cur.execute("INSERT INTO dbo.Transactions (AdvanceRef, CustomerId, Type, Amount, Status, CreatedAt) "
                        "SELECT CAST(AdvanceId AS NVARCHAR(20)), CustomerId, 'disbursement', Amount, 'settled', %s "
                        "FROM dbo.Advances WHERE AdvanceId = %s", (when, aid))
            count("advances_updated")
            count("transactions_inserted")

        funded = ids(cur, "SELECT AdvanceId FROM dbo.Advances WHERE Status = 'funded' ORDER BY AdvanceId")
        for aid in rng.sample(funded, min(14, len(funded))):
            new = "paid_off" if rng.random() < 0.85 else "defaulted"
            cur.execute("UPDATE dbo.Advances SET Status = %s, UpdatedAt = %s WHERE AdvanceId = %s", (new, ts(), aid))
            count("advances_updated")

        # ---- Transactions: weekly repayments (each funded advance pays on its weekday)
        cur.execute("SELECT AdvanceId, CustomerId, Amount * FactorRate / 18.0 FROM dbo.Advances "
                    "WHERE Status = 'funded' ORDER BY AdvanceId")
        repayments = [(str(aid), cid, "repayment", Decimal(amt).quantize(Decimal("0.01")), "pending", ts())
                      for aid, cid, amt in cur.fetchall() if aid % 7 == day % 7]
        config.insert_rows(cur, "dbo.Transactions", ["AdvanceRef", "CustomerId", "Type", "Amount", "Status", "CreatedAt"], repayments)
        count("transactions_inserted", len(repayments))

        # Pending payments settle three days later: "rarely changed" rows DO change while they are young.
        cur.execute("UPDATE dbo.Transactions SET Status = 'settled' WHERE Status = 'pending' AND CreatedAt < %s",
                    (datetime.combine(simdate - timedelta(days=3), time(0)),))
        count("transactions_updated", cur.rowcount)

        # One bounced payment: the settled repayment is reversed and a fee is charged.
        cur.execute("SELECT TOP 50 TransactionId, AdvanceRef, CustomerId FROM dbo.Transactions "
                    "WHERE Type = 'repayment' AND Status = 'settled' ORDER BY TransactionId DESC")
        tid, ref, cid = rng.choice(cur.fetchall())
        cur.execute("UPDATE dbo.Transactions SET Status = 'reversed' WHERE TransactionId = %s", (tid,))
        cur.execute("INSERT INTO dbo.Transactions (AdvanceRef, CustomerId, Type, Amount, Status, CreatedAt) "
                    "VALUES (%s,%s,'fee',25.00,'settled',%s)", (ref, cid, ts()))
        count("transactions_updated")
        count("transactions_inserted")

        # One voided row: a pending repayment posted by mistake is removed.
        voidable = ids(cur, "SELECT TransactionId FROM dbo.Transactions WHERE Status = 'pending' ORDER BY TransactionId")
        if voidable:
            cur.execute("DELETE FROM dbo.Transactions WHERE TransactionId = %s", (rng.choice(voidable),))
            count("transactions_deleted")

        # ---- Cards: new, renewed, removed
        with_adv = ids(cur, "SELECT DISTINCT CustomerId FROM dbo.Advances ORDER BY CustomerId")
        for cid in rng.sample(with_adv, 8):
            now = ts()
            cur.execute("INSERT INTO dbo.Cards (CustomerId, Brand, Last4, ExpMonth, ExpYear, Fingerprint, IsDefault, CreatedAt, UpdatedAt) "
                        "VALUES (%s,%s,%s,%s,%s,%s,0,%s,%s)",
                        (cid, rng.choice(["visa", "mastercard"]), f"{rng.randint(0, 9999):04d}", rng.randint(1, 12),
                         rng.randint(2027, 2031), f"{rng.getrandbits(64):016x}", now, now))
            count("cards_inserted")
        cards = ids(cur, "SELECT CardId FROM dbo.Cards ORDER BY CardId")
        for card in rng.sample(cards, 4):
            cur.execute("UPDATE dbo.Cards SET ExpYear = ExpYear + 4, UpdatedAt = %s WHERE CardId = %s", (ts(), card))
            count("cards_updated")
        removable = ids(cur, "SELECT CardId FROM dbo.Cards WHERE IsDefault = 0 ORDER BY CardId")
        for card in rng.sample(removable, 3):
            cur.execute("DELETE FROM dbo.Cards WHERE CardId = %s", (card,))
            count("cards_deleted")

        cur.execute("UPDATE sim.Clock SET Day = %s, SimDate = %s", (day, simdate))
        conn.commit()

    print(f"Simulated day {day} ({simdate}):")
    for k in sorted(stats):
        print(f"  {k:<40}{stats[k]:>6}")


if __name__ == "__main__":
    main()
