-- ============================================
-- Table: dbo.SchwabQuotes_Raw
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:03
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotes_Raw', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotes_Raw;
GO
CREATE TABLE dbo.SchwabQuotes_Raw (
    Id int NOT NULL IDENTITY(1,1),
    Symbol nvarchar(20) NOT NULL,
    LastPrice decimal(18,4) NULL,
    BidPrice decimal(18,4) NULL,
    AskPrice decimal(18,4) NULL,
    QuoteTime datetime2 NULL,
    RawJson nvarchar(MAX) NULL,
    InsertedAt datetime2 NOT NULL,
);
GO
