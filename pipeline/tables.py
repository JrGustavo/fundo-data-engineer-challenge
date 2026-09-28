"""Single registry of the source tables the pipeline knows about.

Profiling, loading and checks all read from here, so a table's columns and keys
are defined exactly once.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Table:
    name: str
    pk: str
    columns: tuple[str, ...]          # all columns, primary key first
    updated_at: str | None = None     # app-maintained "last modified" column, if any
    created_at: str | None = None     # insert timestamp, if any

    def select_list(self) -> str:
        return ", ".join(self.columns)


TABLES: dict[str, Table] = {t.name: t for t in [
    Table("Customers", "CustomerId",
          ("CustomerId", "FirstName", "LastName", "Email", "Phone", "DateOfBirth", "TaxId",
           "AddressLine", "City", "State", "ZipCode", "CreatedAt", "UpdatedAt"),
          updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("Advances", "AdvanceId",
          ("AdvanceId", "CustomerId", "Amount", "FactorRate", "Status", "FundedAt", "CreatedAt", "UpdatedAt"),
          updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("Transactions", "TransactionId",
          ("TransactionId", "AdvanceRef", "CustomerId", "Type", "Amount", "Status", "CreatedAt"),
          created_at="CreatedAt"),
    Table("Cards", "CardId",
          ("CardId", "CustomerId", "Brand", "Last4", "ExpMonth", "ExpYear", "Fingerprint", "IsDefault",
           "CreatedAt", "UpdatedAt"),
          updated_at="UpdatedAt", created_at="CreatedAt"),
    Table("CustomerHistory", "HistoryId",
          ("HistoryId", "CustomerId", "FieldName", "OldValue", "NewValue", "ChangedAt"),
          created_at="ChangedAt"),
]}

# Present in the source, deliberately never loaded (see SOLUTION.md).
EXCLUDED = ("tmp_marketing_export_2023",)


def row_hash_sql(table: Table) -> str:
    """T-SQL expression: SHA-256 of every column except the key, NULL-safe and delimited."""
    parts = ", N'|', ".join(f"ISNULL(CONVERT(NVARCHAR(MAX), {c}), N'~')" for c in table.columns if c != table.pk)
    return f"CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT({parts})), 2)"
