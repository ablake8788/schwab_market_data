-- ============================================
-- Table: dbo.SchwabQuotesHistory_Raw
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:04
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesHistory_Raw', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesHistory_Raw;
GO
CREATE TABLE dbo.SchwabQuotesHistory_Raw (
    Id int NOT NULL IDENTITY(1,1),
    Symbol nvarchar(20) NOT NULL,
    BarDateTime datetime2 NOT NULL,
    OpenPrice decimal(18,8) NULL,
    HighPrice decimal(18,8) NULL,
    LowPrice decimal(18,8) NULL,
    ClosePrice decimal(18,8) NULL,
    Volume bigint NULL,
    RawJson nvarchar(MAX) NULL,
    LoadDate datetime2 NOT NULL,
);
GO
