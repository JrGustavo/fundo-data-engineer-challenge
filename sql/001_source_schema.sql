-- Source schema, shaped like Fundo's operational database.
-- Deliberately includes the problems described in the challenge:
--   * Transactions.AdvanceRef: an identifier stored as unbounded text (NVARCHAR(MAX)),
--     with no foreign key, so it cannot be indexed and joins need casting.
--   * Transactions has no UpdatedAt column: "rarely changed" rows cannot be detected by timestamp.
--   * Customers.UpdatedAt is maintained by the application, not by the database,
--     so some writers (support scripts) forget to bump it.
--   * tmp_marketing_export_2023: scratch heap table nobody owns (no primary key).

CREATE TABLE dbo.Customers (
    CustomerId   INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Customers PRIMARY KEY,
    FirstName    NVARCHAR(100) NOT NULL,
    LastName     NVARCHAR(100) NOT NULL,
    Email        NVARCHAR(255) NULL,
    Phone        NVARCHAR(50)  NULL,
    DateOfBirth  DATE          NULL,
    TaxId        VARCHAR(11)   NULL,   -- synthetic SSN/ITIN-like value, 9xx-xx-xxxx
    AddressLine  NVARCHAR(200) NULL,
    City         NVARCHAR(100) NULL,
    State        CHAR(2)       NULL,
    ZipCode      VARCHAR(10)   NULL,
    CreatedAt    DATETIME2(0)  NOT NULL,
    UpdatedAt    DATETIME2(0)  NOT NULL
);
GO

CREATE TABLE dbo.Advances (
    AdvanceId    INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Advances PRIMARY KEY,
    CustomerId   INT NOT NULL CONSTRAINT FK_Advances_Customers REFERENCES dbo.Customers(CustomerId),
    Amount       DECIMAL(12,2) NOT NULL,
    FactorRate   DECIMAL(4,2)  NOT NULL,
    Status       VARCHAR(20)   NOT NULL,  -- pending, approved, declined, funded, paid_off, defaulted
    FundedAt     DATETIME2(0)  NULL,
    CreatedAt    DATETIME2(0)  NOT NULL,
    UpdatedAt    DATETIME2(0)  NOT NULL
);
GO

CREATE TABLE dbo.Transactions (
    TransactionId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Transactions PRIMARY KEY,
    AdvanceRef    NVARCHAR(MAX) NULL,     -- BAD: identifier as unbounded text, no FK
    CustomerId    INT           NOT NULL,
    Type          VARCHAR(20)   NOT NULL, -- disbursement, repayment, fee
    Amount        DECIMAL(12,2) NOT NULL,
    Status        VARCHAR(20)   NOT NULL, -- pending, settled, reversed
    CreatedAt     DATETIME2(0)  NOT NULL
);
GO

CREATE TABLE dbo.Cards (
    CardId       INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Cards PRIMARY KEY,
    CustomerId   INT NOT NULL CONSTRAINT FK_Cards_Customers REFERENCES dbo.Customers(CustomerId),
    Brand        VARCHAR(20)  NOT NULL,
    Last4        CHAR(4)      NOT NULL,
    ExpMonth     TINYINT      NOT NULL,
    ExpYear      SMALLINT     NOT NULL,
    Fingerprint  CHAR(16)     NOT NULL,   -- processor fingerprint: same physical card => same value
    IsDefault    BIT          NOT NULL,
    CreatedAt    DATETIME2(0) NOT NULL,
    UpdatedAt    DATETIME2(0) NOT NULL
);
GO

-- Append-only audit trail: rows are inserted, never updated or deleted.
CREATE TABLE dbo.CustomerHistory (
    HistoryId    BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_CustomerHistory PRIMARY KEY,
    CustomerId   INT           NOT NULL,
    FieldName    VARCHAR(50)   NOT NULL,
    OldValue     NVARCHAR(400) NULL,
    NewValue     NVARCHAR(400) NULL,
    ChangedAt    DATETIME2(0)  NOT NULL
);
GO

-- Scratch table: a one-off export from 2023 that nobody owns or reads.
CREATE TABLE dbo.tmp_marketing_export_2023 (
    Email        NVARCHAR(255) NULL,
    FirstName    NVARCHAR(100) NULL,
    Segment      VARCHAR(20)   NULL,
    ExportedAt   DATETIME2(0)  NULL
);
GO

-- Simulation bookkeeping (not part of Fundo's schema; never loaded to the warehouse).
CREATE SCHEMA sim;
GO
CREATE TABLE sim.Clock (Day INT NOT NULL, SimDate DATE NOT NULL);
GO
