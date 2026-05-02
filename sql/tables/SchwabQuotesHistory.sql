-- ============================================
-- Table: dbo.SchwabQuotesHistory
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:04
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesHistory', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesHistory;
GO
CREATE TABLE dbo.SchwabQuotesHistory (
    HistoryId int NOT NULL IDENTITY(1,1),
    RawId int NOT NULL,
    Symbol nvarchar(20) NOT NULL,
    BarDateTime datetime2 NOT NULL,
    OpenPrice decimal(18,4) NULL,
    HighPrice decimal(18,4) NULL,
    LowPrice decimal(18,4) NULL,
    ClosePrice decimal(18,4) NULL,
    Volume bigint NULL,
    InsertedAt datetime2 NOT NULL,
    OperationType nvarchar(150) NULL,
    Comments nvarchar(150) NULL,
    BatchId nvarchar(50) NULL,
    LoadDate datetime NOT NULL,
    CreatedBy nvarchar(50) NULL,
    CreatedOn datetime NULL,
    LastEditBy nvarchar(50) NULL,
    LastEditOn datetime NULL,
    SysStartTime datetime2 NOT NULL,
    SysEndTime datetime2 NOT NULL,
);
GO
