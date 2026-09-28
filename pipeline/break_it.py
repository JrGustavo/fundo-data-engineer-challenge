"""Damage the warehouse on purpose, one example of each failure `make check` must catch.

Only the warehouse is touched (never the source), and no watermark moves, exactly like
a buggy loader or a manual edit would behave. `make repair` puts it back.
"""
from pipeline import config


def main() -> None:
    wh = config.warehouse()
    lost = [r[0] for r in wh.execute("SELECT CustomerId FROM raw.Customers ORDER BY CustomerId LIMIT 3").fetchall()]
    wh.execute("DELETE FROM raw.Customers WHERE CustomerId IN (SELECT CustomerId FROM raw.Customers ORDER BY CustomerId LIMIT 3)")
    print(f"  missing    : deleted Customers {lost} from the warehouse (a gap)")

    phantom = wh.execute("SELECT MAX(CardId) + 100000 FROM raw.Cards").fetchone()[0]
    wh.execute(f"INSERT INTO raw.Cards SELECT {phantom}, * EXCLUDE (CardId) FROM raw.Cards ORDER BY CardId LIMIT 1")
    print(f"  extra      : inserted Card {phantom}, which does not exist in the source (a delete that never arrived)")

    stale = [r[0] for r in wh.execute("SELECT TransactionId FROM raw.Transactions WHERE Status = 'settled' "
                                      "ORDER BY TransactionId LIMIT 5").fetchall()]
    wh.execute(f"UPDATE raw.Transactions SET Status = 'pending' WHERE TransactionId IN ({', '.join(map(str, stale))})")
    wh.execute("UPDATE raw.Advances SET Amount = Amount + 1 WHERE AdvanceId = (SELECT MIN(AdvanceId) FROM raw.Advances)")
    print(f"  mismatched : Transactions {stale} set back to 'pending'; the first Advance amount changed by $1")
    wh.close()
    print("\nWarehouse damaged. Run `make check` to see it caught.")


if __name__ == "__main__":
    main()
