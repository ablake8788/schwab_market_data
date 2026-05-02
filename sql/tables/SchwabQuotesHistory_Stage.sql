-- ============================================
-- Table: dbo.SchwabQuotesHistory_Stage
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:04
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesHistory_Stage', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesHistory_Stage;
GO
CREATE TABLE dbo.SchwabQuotesHistory_Stage (
    StageId int NOT NULL IDENTITY(1,1),
    RawId int NOT NULL,
    Symbol nvarchar(20) NOT NULL,
    BarDateTime datetime2 NOT NULL,
    OpenPrice decimal(18,4) NULL,
    HighPrice decimal(18,4) NULL,
    LowPrice decimal(18,4) NULL,
    ClosePrice decimal(18,4) NULL,
    Volume bigint NULL,
    RawJson nvarchar(MAX) NULL,
    InsertedAt datetime2 NOT NULL,
);
GO
