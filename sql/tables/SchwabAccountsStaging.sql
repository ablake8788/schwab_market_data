-- ============================================
-- Table: dbo.SchwabAccountsStaging
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:02
-- ============================================

IF OBJECT_ID('dbo.SchwabAccountsStaging', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabAccountsStaging;
GO
CREATE TABLE dbo.SchwabAccountsStaging (
    AccountNumber nvarchar(50) NOT NULL,
    AccountType nvarchar(50) NULL,
    Nickname nvarchar(200) NULL,
    Status nvarchar(50) NULL,
    IsMargin int NULL,
    RawJson nvarchar(MAX) NULL,
    ExtractedAt datetime NOT NULL,
    OperationType nvarchar(150) NULL,
    Comments nvarchar(150) NULL,
    BatchId nvarchar(50) NULL,
    LoadDate datetime NULL,
    CreatedBy nvarchar(50) NOT NULL,
    CreatedOn datetime NOT NULL,
    LastEditBy nvarchar(50) NULL,
    LastEditOn datetime NULL,
);
GO
