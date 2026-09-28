"""Recreate the source database with deterministic synthetic data.

The data is small but deliberately messy. Every "trap" the challenge describes is
planted on purpose and reported at the end, so later steps can be verified against
known answers:

  * true duplicates (same TaxId) — some with a funded advance on the *newer* record,
    and some where two records in a group each hold a funded/paid-off advance
  * look-alikes that are NOT the same person: couples sharing an email/address,
    recycled phone numbers, namesakes
  * test accounts, plus traps a naive filter would catch: surname "Testerman",
    staff with @fundo.com emails who have real advances
  * malformed phones and emails

Same seed => same data on every machine, so numbers in SOLUTION.md are reproducible.
"""
import os
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pipeline import config

SEED = 42
NOW = datetime(2026, 9, 27, 6, 0, 0)   # fixed "today" so every run is identical
START = datetime(2019, 3, 1)           # Fundo was founded in 2019

N_PEOPLE = 6000

FIRST = ["James", "Maria", "Jose", "Ana", "Michael", "Carlos", "Jennifer", "Luis", "David", "Sofia",
         "Robert", "Daniela", "John", "Valentina", "William", "Camila", "Richard", "Isabella", "Joseph",
         "Lucia", "Thomas", "Gabriela", "Charles", "Andrea", "Daniel", "Paula", "Matthew", "Carolina",
         "Anthony", "Natalia", "Mark", "Laura", "Steven", "Diana", "Kevin", "Monica", "Brian", "Elena",
         "Jorge", "Patricia", "Miguel", "Rosa", "Pedro", "Teresa", "Juan", "Claudia", "Andres", "Marta"]
LAST = ["Smith", "Garcia", "Johnson", "Rodriguez", "Williams", "Martinez", "Brown", "Hernandez",
        "Jones", "Lopez", "Miller", "Gonzalez", "Davis", "Perez", "Wilson", "Sanchez", "Anderson",
        "Ramirez", "Taylor", "Torres", "Thomas", "Flores", "Moore", "Rivera", "Jackson", "Gomez",
        "Martin", "Diaz", "Lee", "Reyes", "Thompson", "Morales", "White", "Cruz", "Harris", "Ortiz",
        "Clark", "Gutierrez", "Lewis", "Chavez", "Walker", "Ramos", "Hall", "Castillo", "Young", "Vargas"]
NICKNAMES = {"James": "Jim", "Michael": "Mike", "Robert": "Bob", "William": "Bill", "Joseph": "Joe",
             "Richard": "Rick", "Daniel": "Dan", "Anthony": "Tony", "Jose": "Pepe", "Steven": "Steve",
             "Maria": "Mari", "Jennifer": "Jen", "Gabriela": "Gaby", "Patricia": "Paty"}
CITIES = [("Miami", "FL", "331"), ("Orlando", "FL", "328"), ("Tampa", "FL", "336"), ("Houston", "TX", "770"),
          ("Dallas", "TX", "752"), ("Atlanta", "GA", "303"), ("Phoenix", "AZ", "850"),
          ("Chicago", "IL", "606"), ("Charlotte", "NC", "282"), ("Las Vegas", "NV", "891")]
AREA = {"FL": ["305", "786", "407", "813"], "TX": ["713", "214", "832"], "GA": ["404", "678"],
        "AZ": ["602", "480"], "IL": ["312", "773"], "NC": ["704"], "NV": ["702"]}
STREETS = ["Main St", "Oak Ave", "Palm Dr", "Coral Way", "Maple St", "Pine Rd", "Bay Blvd", "Cedar Ln",
           "Sunset Ave", "Lake Dr", "Park Ave", "Elm St"]
PROVIDERS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]


def rand_dt(rng: random.Random, lo: datetime, hi: datetime) -> datetime:
    return lo + timedelta(seconds=rng.randint(0, int((hi - lo).total_seconds())))


def fmt_phone(rng: random.Random, digits: str) -> str:
    """Same valid number, written the many ways real users type it."""
    a, b, c = digits[:3], digits[3:6], digits[6:]
    return rng.choice([f"({a}) {b}-{c}", f"{a}-{b}-{c}", f"{a}{b}{c}", f"+1 {a} {b} {c}", f"{a}.{b}.{c}"])


class Generator:
    def __init__(self) -> None:
        self.rng = random.Random(SEED)
        self.customers: list[dict] = []
        self.used_taxids: set[str] = set()
        self.planted: dict[str, list] = {}

    # ---------- customers ----------
    def taxid(self) -> str:
        while True:
            t = f"9{self.rng.randint(0, 99):02d}-{self.rng.randint(70, 99)}-{self.rng.randint(0, 9999):04d}"
            if t not in self.used_taxids:
                self.used_taxids.add(t)
                return t

    def person(self, created: datetime, **over) -> dict:
        r = self.rng
        first, last = r.choice(FIRST), r.choice(LAST)
        city, state, zip3 = r.choice(CITIES)
        digits = r.choice(AREA[state]) + f"{r.randint(200, 999)}{r.randint(0, 9999):04d}"
        p = dict(
            FirstName=first, LastName=last,
            Email=f"{first.lower()}.{last.lower()}{r.randint(1, 999)}@{r.choice(PROVIDERS)}",
            Phone=fmt_phone(r, digits), phone_digits=digits,
            DateOfBirth=date(r.randint(1960, 2003), r.randint(1, 12), r.randint(1, 28)),
            TaxId=self.taxid(),
            AddressLine=f"{r.randint(10, 9999)} {r.choice(STREETS)}", City=city, State=state,
            ZipCode=f"{zip3}{r.randint(10, 99)}", CreatedAt=created, UpdatedAt=created,
            kind="person",
        )
        p.update(over)
        return p

    def add(self, c: dict) -> dict:
        c["CustomerId"] = len(self.customers) + 1
        self.customers.append(c)
        return c

    def build_customers(self) -> None:
        r = self.rng
        # Base population, created in chronological order (ids grow with time, like production).
        created = sorted(rand_dt(r, START, NOW - timedelta(days=30)) for _ in range(N_PEOPLE))
        base = [self.add(self.person(ts)) for ts in created]

        # True duplicates: same person re-registers later (often to get a second advance).
        originals = r.sample(base, 210)
        dup_groups = []
        for i, orig in enumerate(originals):
            extra = 2 if i < 30 else 1
            group = [orig]
            for _ in range(extra):
                d = dict(orig)
                d["kind"] = "duplicate"
                d["CreatedAt"] = d["UpdatedAt"] = rand_dt(r, orig["CreatedAt"], NOW - timedelta(days=5))
                user, _, dom = orig["Email"].partition("@")
                d["Email"] = r.choice([orig["Email"].upper(), f"{user}+fundo@{dom}",
                                       f"{user}@{r.choice(PROVIDERS)}", orig["Email"]])
                d["Phone"] = fmt_phone(r, orig["phone_digits"])
                if r.random() < 0.15:
                    d["FirstName"] = NICKNAMES.get(orig["FirstName"], orig["FirstName"])
                if r.random() < 0.25:
                    d["TaxId"] = None          # onboarding flow that skipped KYC
                if r.random() < 0.20:
                    city, state, zip3 = r.choice(CITIES)
                    d.update(City=city, State=state, AddressLine=f"{r.randint(10, 9999)} {r.choice(STREETS)}",
                             ZipCode=f"{zip3}{r.randint(10, 99)}")
                group.append(self.add(d))
            dup_groups.append(group)
        self.dup_groups = dup_groups

        # Look-alikes that are NOT the same person.
        couples = []
        for orig in r.sample(base, 15):   # spouses sharing email, address and last name
            s = self.person(rand_dt(r, orig["CreatedAt"], NOW - timedelta(days=5)),
                            LastName=orig["LastName"], Email=orig["Email"], AddressLine=orig["AddressLine"],
                            City=orig["City"], State=orig["State"], ZipCode=orig["ZipCode"], kind="spouse")
            couples.append((orig["CustomerId"], self.add(s)["CustomerId"]))
        recycled = []
        for a, b in zip(r.sample(base, 10), r.sample(base, 10)):   # carrier reassigned the number
            if a is not b and b["kind"] == "person":
                b["Phone"], b["phone_digits"], b["kind"] = fmt_phone(r, a["phone_digits"]), a["phone_digits"], "recycled_phone"
                recycled.append((a["CustomerId"], b["CustomerId"]))
        namesakes = []
        for orig in r.sample(base, 8):
            n = self.person(rand_dt(r, START, NOW - timedelta(days=5)),
                            FirstName=orig["FirstName"], LastName=orig["LastName"], kind="namesake")
            namesakes.append((orig["CustomerId"], self.add(n)["CustomerId"]))

        # Test accounts (to exclude) and the traps a naive filter would catch.
        tests = []
        test_specs = [("Test", "User", "test@fundo.com"), ("Test", "Account", "test2@fundo.com"),
                      ("QA", "Tester", "qa@fundo.com"), ("Qa", "Automation", "qa+1@fundo.com"),
                      ("Qa", "Automation", "qa+2@fundo.com"), ("Demo", "Merchant", "demo@fundo.com"),
                      ("Test", "Test", "test@test.com"), ("John", "Doe", "john.doe@example.com"),
                      ("Jane", "Doe", "jane.doe@example.com"), ("Asdf", "Asdf", "asdf@asdf.com"),
                      ("Test", "Staging", "staging+test@fundo.com"), ("Load", "Test", "loadtest1@mailinator.com"),
                      ("Load", "Test", "loadtest2@mailinator.com"), ("Sandbox", "Merchant", "sandbox@example.com")]
        for fn, ln, em in test_specs:
            t = self.person(rand_dt(r, START, NOW - timedelta(days=5)), FirstName=fn, LastName=ln, Email=em,
                            Phone="(305) 555-0100", TaxId=None, kind="test")
            tests.append(self.add(t)["CustomerId"])
        traps = []
        for fn in ["Robert", "Laura", "Kevin"]:            # real people named Testerman
            t = self.person(rand_dt(r, START, NOW - timedelta(days=30)), FirstName=fn, LastName="Testerman", kind="trap_real")
            traps.append(self.add(t)["CustomerId"])
        t = self.person(rand_dt(r, START, NOW - timedelta(days=30)), FirstName="Celeste", LastName="Testa", kind="trap_real")
        traps.append(self.add(t)["CustomerId"])
        staff = []
        for fn, ln in [("Maria", "Gomez"), ("Daniel", "Reyes"), ("Andrea", "Cruz"), ("Steven", "Hall")]:
            t = self.person(rand_dt(r, START, NOW - timedelta(days=60)), FirstName=fn, LastName=ln,
                            Email=f"{fn.lower()}.{ln.lower()}@fundo.com", kind="staff_real")
            staff.append(self.add(t)["CustomerId"])

        # Malformed contact data on ordinary customers.
        bad_emails = ["{u}@gmail", "{u}@@yahoo.com", "{u} @outlook.com", "{u}..x@hotmail.com", "none", "", "{u}.gmail.com"]
        bad_phones = ["555-0199", "305555", "N/A", "(305) 555-12345", "000-000-0000", "call after 5pm", "1234567890123"]
        pool = [c for c in self.customers if c["kind"] in ("person", "duplicate")]
        malformed_email = r.sample(pool, 150)
        malformed_phone = r.sample(pool, 140)
        for c in malformed_email:
            c["Email"] = r.choice(bad_emails).format(u=c["FirstName"].lower())
        for c in malformed_phone:
            c["Phone"] = r.choice(bad_phones)

        self.planted = dict(
            duplicate_groups=len(dup_groups), duplicate_extra_rows=sum(len(g) - 1 for g in dup_groups),
            couples_sharing_email=len(couples), recycled_phones=len(recycled), namesakes=len(namesakes),
            test_accounts=len(tests), testerman_like_real=len(traps), staff_with_fundo_email=len(staff),
            malformed_emails=len(malformed_email), malformed_phones=len(malformed_phone),
        )

    # ---------- advances, transactions, cards ----------
    def build_money(self) -> None:
        r = self.rng
        self.advances, self.transactions, self.cards = [], [], []
        forced: dict[int, list[str]] = {}

        # Force the interesting duplicate cases the challenge asks for.
        g = self.dup_groups
        forced[g[0][1]["CustomerId"]] = ["funded"]                      # newer record holds the funded advance
        forced[g[0][0]["CustomerId"]] = []
        forced[g[1][0]["CustomerId"]] = ["paid_off"]                    # two protected records in one group
        forced[g[1][1]["CustomerId"]] = ["funded"]
        forced[g[2][0]["CustomerId"]] = ["funded"]
        forced[g[2][1]["CustomerId"]] = ["paid_off"]
        self.planted["dup_group_newer_record_funded"] = [c["CustomerId"] for c in g[0]]
        self.planted["dup_groups_two_protected"] = [[c["CustomerId"] for c in g[1]], [c["CustomerId"] for c in g[2]]]

        for c in self.customers:
            cid, kind = c["CustomerId"], c["kind"]
            if cid in forced:
                statuses = forced[cid]
            elif kind == "test":
                statuses = [r.choice(["pending", "declined"])] if r.random() < 0.6 else []
            elif kind == "staff_real":
                statuses = ["paid_off", "funded"]
            else:
                p_has = 0.35 if kind == "duplicate" else 0.72
                if r.random() >= p_has:
                    statuses = []
                else:
                    n = r.choices([1, 2, 3], [0.6, 0.3, 0.1])[0]
                    statuses = [r.choices(["paid_off", "declined", "defaulted"], [0.8, 0.12, 0.08])[0] for _ in range(n - 1)]
                    statuses.append(r.choices(["paid_off", "funded", "approved", "pending", "declined", "defaulted"],
                                              [0.35, 0.30, 0.05, 0.12, 0.12, 0.06])[0])
            cursor = c["CreatedAt"]
            for i, status in enumerate(statuses):
                self._advance(c, status, cursor, last=(i == len(statuses) - 1))
                cursor = self.advances[-1]["CreatedAt"] + timedelta(days=r.randint(30, 200))
                if cursor > NOW - timedelta(days=10):
                    cursor = NOW - timedelta(days=10)

            if kind != "test" and (statuses or r.random() < 0.2):
                self._cards(c)

        # Duplicates usually re-add the same physical card; couples sometimes share one.
        by_customer = {}
        for card in self.cards:
            by_customer.setdefault(card["CustomerId"], []).append(card)
        for group in self.dup_groups:
            src = by_customer.get(group[0]["CustomerId"])
            for d in group[1:]:
                if src and d["CustomerId"] in by_customer and r.random() < 0.7:
                    by_customer[d["CustomerId"]][0]["Fingerprint"] = src[0]["Fingerprint"]
                    by_customer[d["CustomerId"]][0]["Last4"] = src[0]["Last4"]

    def _advance(self, c: dict, status: str, earliest: datetime, last: bool) -> None:
        r = self.rng
        weeks = r.randint(12, 26)
        if status in ("paid_off", "defaulted") and earliest > NOW - timedelta(weeks=weeks + 1):
            status = "funded"   # customer is too recent to have finished an advance
        if status == "funded":
            hi = NOW - timedelta(days=3)
            funded_at = rand_dt(r, min(max(earliest, NOW - timedelta(weeks=weeks - 1)), hi), hi)
        elif status in ("paid_off", "defaulted"):
            hi = NOW - timedelta(weeks=weeks + 1)
            funded_at = rand_dt(r, min(earliest, hi - timedelta(days=1)), hi)
        else:
            funded_at = None
        created = (funded_at - timedelta(days=r.randint(1, 5))) if funded_at else rand_dt(r, max(earliest, NOW - timedelta(days=60)), NOW - timedelta(days=1))
        amount = Decimal(r.choice([500, 750, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 7500, 10000]))
        factor = Decimal(str(round(r.uniform(1.15, 1.49), 2)))
        adv = dict(AdvanceId=len(self.advances) + 1, CustomerId=c["CustomerId"], Amount=amount, FactorRate=factor,
                   Status=status, FundedAt=funded_at, CreatedAt=created, UpdatedAt=funded_at or created)
        self.advances.append(adv)
        if funded_at:
            self._transactions(adv, weeks)

    def _ref(self, advance_id: int) -> str:
        """AdvanceRef is text: mostly clean, sometimes padded or with stray spaces."""
        x = self.rng.random()
        if x < 0.02:
            return f" {advance_id} "
        if x < 0.03:
            return f"{advance_id:08d}"
        return str(advance_id)

    def _transactions(self, adv: dict, weeks: int) -> None:
        r = self.rng
        cid, aid, funded = adv["CustomerId"], adv["AdvanceId"], adv["FundedAt"]
        self.transactions.append(dict(AdvanceRef=self._ref(aid), CustomerId=cid, Type="disbursement",
                                      Amount=adv["Amount"], Status="settled", CreatedAt=funded))
        payback = adv["Amount"] * adv["FactorRate"]
        weekly = (payback / weeks).quantize(Decimal("0.01"))
        stop = weeks if adv["Status"] == "paid_off" else r.randint(3, weeks - 2) if adv["Status"] == "defaulted" else weeks
        for w in range(1, stop + 1):
            ts = funded + timedelta(weeks=w, hours=r.randint(0, 6))
            if ts > NOW:
                break
            status = "pending" if (NOW - ts).days < 3 else "settled"
            if status == "settled" and r.random() < 0.02:   # bounced payment (NSF) + fee
                self.transactions.append(dict(AdvanceRef=self._ref(aid), CustomerId=cid, Type="repayment",
                                              Amount=weekly, Status="reversed", CreatedAt=ts))
                self.transactions.append(dict(AdvanceRef=self._ref(aid), CustomerId=cid, Type="fee",
                                              Amount=Decimal("25.00"), Status="settled", CreatedAt=ts + timedelta(hours=1)))
                continue
            self.transactions.append(dict(AdvanceRef=self._ref(aid), CustomerId=cid, Type="repayment",
                                          Amount=weekly, Status=status, CreatedAt=ts))

    def _cards(self, c: dict) -> None:
        r = self.rng
        for i in range(r.choices([1, 2], [0.8, 0.2])[0]):
            ts = rand_dt(r, c["CreatedAt"], NOW - timedelta(days=1))
            self.cards.append(dict(CustomerId=c["CustomerId"], Brand=r.choice(["visa", "mastercard", "amex", "discover"]),
                                   Last4=f"{r.randint(0, 9999):04d}", ExpMonth=r.randint(1, 12), ExpYear=r.randint(2026, 2031),
                                   Fingerprint=f"{r.getrandbits(64):016x}", IsDefault=(i == 0), CreatedAt=ts, UpdatedAt=ts))

    # ---------- history + scratch ----------
    def build_history(self) -> None:
        r = self.rng
        self.history = []
        for c in r.sample(self.customers, 2500):
            ts = c["CreatedAt"]
            for _ in range(r.randint(1, 4)):
                ts = rand_dt(r, ts, NOW - timedelta(days=1))
                field = r.choice(["Email", "Phone", "AddressLine"])
                self.history.append(dict(CustomerId=c["CustomerId"], FieldName=field, OldValue=f"old {field.lower()}",
                                         NewValue=c[field], ChangedAt=ts))
        self.history.sort(key=lambda h: h["ChangedAt"])
        self.scratch = [dict(Email=c["Email"], FirstName=c["FirstName"], Segment=r.choice(["winback", "upsell", "churned"]),
                             ExportedAt=datetime(2023, 5, 17, 14, 0)) for c in r.sample(self.customers, 2000)]

    def build(self) -> "Generator":
        self.build_customers()
        self.build_money()
        self.build_history()
        self.transactions.sort(key=lambda t: t["CreatedAt"])
        return self


CUSTOMER_COLS = ["CustomerId", "FirstName", "LastName", "Email", "Phone", "DateOfBirth", "TaxId",
                 "AddressLine", "City", "State", "ZipCode", "CreatedAt", "UpdatedAt"]
ADVANCE_COLS = ["AdvanceId", "CustomerId", "Amount", "FactorRate", "Status", "FundedAt", "CreatedAt", "UpdatedAt"]
TXN_COLS = ["AdvanceRef", "CustomerId", "Type", "Amount", "Status", "CreatedAt"]
CARD_COLS = ["CustomerId", "Brand", "Last4", "ExpMonth", "ExpYear", "Fingerprint", "IsDefault", "CreatedAt", "UpdatedAt"]
HIST_COLS = ["CustomerId", "FieldName", "OldValue", "NewValue", "ChangedAt"]
SCRATCH_COLS = ["Email", "FirstName", "Segment", "ExportedAt"]


def rows(items: list[dict], cols: list[str]) -> list[tuple]:
    return [tuple(i[c] for c in cols) for i in items]


def main() -> None:
    db = os.environ["SRC_DB"]
    print(f"Recreating source database {db} (local synthetic data only)...")
    with config.source(database="master", autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(f"IF DB_ID('{db}') IS NOT NULL BEGIN "
                    f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END")
        cur.execute(f"CREATE DATABASE [{db}]")

    gen = Generator().build()
    with config.source() as conn:
        cur = conn.cursor()
        config.run_sql_file(cur, config.ROOT / "sql" / "001_source_schema.sql")
        config.insert_rows(cur, "dbo.Customers", CUSTOMER_COLS, rows(gen.customers, CUSTOMER_COLS), identity=True)
        config.insert_rows(cur, "dbo.Advances", ADVANCE_COLS, rows(gen.advances, ADVANCE_COLS), identity=True)
        config.insert_rows(cur, "dbo.Transactions", TXN_COLS, rows(gen.transactions, TXN_COLS))
        config.insert_rows(cur, "dbo.Cards", CARD_COLS, rows(gen.cards, CARD_COLS))
        config.insert_rows(cur, "dbo.CustomerHistory", HIST_COLS, rows(gen.history, HIST_COLS))
        config.insert_rows(cur, "dbo.tmp_marketing_export_2023", SCRATCH_COLS, rows(gen.scratch, SCRATCH_COLS))
        cur.execute("INSERT INTO sim.Clock (Day, SimDate) VALUES (0, %s)", (NOW.date(),))
        conn.commit()

        print("\nRows per table:")
        for t in ["Customers", "Advances", "Transactions", "Cards", "CustomerHistory", "tmp_marketing_export_2023"]:
            cur.execute(f"SELECT COUNT(*) FROM dbo.{t}")
            print(f"  {t:<28}{cur.fetchone()[0]:>9,}")

    print("\nPlanted cases (known answers for later steps):")
    for k, v in gen.planted.items():
        print(f"  {k:<32}{v}")

    wh = Path(os.environ["WAREHOUSE_PATH"])
    if wh.exists():
        wh.unlink()
        print("\nWarehouse file removed: it described the previous source and would be inconsistent.")


if __name__ == "__main__":
    main()
