"""Single registry of the source tables the pipeline knows about.

Profiling, loading and checks all read from here, so a table's columns, types,
keys and load strategy are defined exactly once.
"""
from dataclasses import dataclass

CHANGE_TRACKING = "change_tracking"   # SQL Server CT: net inserts/updates/deletes since a version
APPEND_ONLY = "append_only"           # high-water mark on an increasing key; rows never change


@dataclass(frozen=True)
class Table:
    name: str
    pk: str
    columns: tuple[tuple[str, str], ...]   # (column, warehouse type), primary key first
    strategy: str
    updated_at: str | None = None          # app-maintained "last modified" column, if any
    created_at: str | None = None          # insert timestamp, if any

    @property
    def names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def warehouse_ddl(self, schema: str = "raw") -> str:
        cols = ",\n    ".join(f"{c} {t}" for c, t in self.columns)
        return (f"CREATE TABLE IF NOT EXISTS {schema}.{self.name} (\n    {cols},\n"
                f"    _source_version BIGINT,\n    _loaded_at TIMESTAMP,\n    PRIMARY KEY ({self.pk})\n)")


TABLES: dict[str, Table] = {t.name: t for t in [
    Table("Customers", "CustomerId",
          (("CustomerId", "INTEGER"), ("FirstName", "VARCHAR"), ("LastName", "VARCHAR"), ("Email", "VARCHAR"),
           ("Phone", "VARCHAR"), ("DateOfBirth", "DATE"), ("TaxId", "VARCHAR"), ("AddressLine", "VARCHAR"),
           ("City", "VARCHAR"), ("State", "VARCHAR"), ("ZipCode", "VARCHAR"), ("CreatedAt", "TIMESTAMP"),
           ("UpdatedAt", "TIMESTAMP"), ("MergedIntoCustomerId", "INTEGER"), ("IdentityHold", "VARCHAR")),
          CHANGE_TRACKING, updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("Advances", "AdvanceId",
          (("AdvanceId", "INTEGER"), ("CustomerId", "INTEGER"), ("Amount", "DECIMAL(12,2)"),
           ("FactorRate", "DECIMAL(4,2)"), ("Status", "VARCHAR"), ("FundedAt", "TIMESTAMP"),
           ("CreatedAt", "TIMESTAMP"), ("UpdatedAt", "TIMESTAMP")),
          CHANGE_TRACKING, updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("Transactions", "TransactionId",
          (("TransactionId", "BIGINT"), ("AdvanceRef", "VARCHAR"), ("CustomerId", "INTEGER"), ("Type", "VARCHAR"),
           ("Amount", "DECIMAL(12,2)"), ("Status", "VARCHAR"), ("CreatedAt", "TIMESTAMP")),
          CHANGE_TRACKING, created_at="CreatedAt"),
    Table("Cards", "CardId",
          (("CardId", "INTEGER"), ("CustomerId", "INTEGER"), ("Brand", "VARCHAR"), ("Last4", "VARCHAR"),
           ("ExpMonth", "SMALLINT"), ("ExpYear", "SMALLINT"), ("Fingerprint", "VARCHAR"), ("IsDefault", "BOOLEAN"),
           ("CreatedAt", "TIMESTAMP"), ("UpdatedAt", "TIMESTAMP")),
          CHANGE_TRACKING, updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("CustomerHistory", "HistoryId",
          (("HistoryId", "BIGINT"), ("CustomerId", "INTEGER"), ("FieldName", "VARCHAR"), ("OldValue", "VARCHAR"),
           ("NewValue", "VARCHAR"), ("ChangedAt", "TIMESTAMP")),
          APPEND_ONLY, created_at="ChangedAt"),
]}

# Present in the source, deliberately never loaded (see SOLUTION.md).
EXCLUDED = ("tmp_marketing_export_2023",)


def row_hash_sql(table: Table, alias: str = "") -> str:
    """T-SQL expression: SHA-256 of every column except the key, NULL-safe and delimited."""
    p = f"{alias}." if alias else ""
    parts = ", N'|', ".join(f"ISNULL(CONVERT(NVARCHAR(MAX), {p}{c}), N'~')" for c in table.names if c != table.pk)
    return f"CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT({parts})), 2)"
