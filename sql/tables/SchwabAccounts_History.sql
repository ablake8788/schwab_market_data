-- ============================================
-- Table: dbo.SchwabAccounts_History
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:02
-- ============================================

IF OBJECT_ID('dbo.SchwabAccounts_History', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabAccounts_History;
GO
CREATE TABLE dbo.SchwabAccounts_History (
    Id uniqueidentifier NOT NULL,
    AccountNumber nvarchar(50) NOT NULL,
    AccountType nvarchar(50) NULL,
    Nickname nvarchar(200) NULL,
    Status nvarchar(50) NULL,
    IsMargin int NULL,
    OperationType nvarchar(150) NULL,
    Comments nvarchar(150) NULL,
    BatchId nvarchar(50) NULL,
    LoadDate datetime NULL,
    CreatedBy nvarchar(50) NULL,
    CreatedOn datetime NULL,
    LastEditBy nvarchar(50) NULL,
    LastEditOn datetime NULL,
    SysStartTime datetime2 NOT NULL,
    SysEndTime datetime2 NOT NULL,
);
GO
