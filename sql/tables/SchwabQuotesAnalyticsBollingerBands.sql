-- ============================================
-- Table: dbo.SchwabQuotesAnalyticsBollingerBands
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:03
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesAnalyticsBollingerBands', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesAnalyticsBollingerBands;
GO
CREATE TABLE dbo.SchwabQuotesAnalyticsBollingerBands (
    Id int NOT NULL IDENTITY(1,1),
    Symbol nvarchar(20) NOT NULL,
    QuoteDate datetime2 NOT NULL,
    ColumnName nvarchar(50) NOT NULL,
    WindowSize int NOT NULL,
    NumStdDev int NOT NULL,
    RawValue decimal(18,8) NOT NULL,
    MiddleBand decimal(18,8) NOT NULL,
    UpperBand decimal(18,8) NOT NULL,
    LowerBand decimal(18,8) NOT NULL,
    OperationType nvarchar(150) NULL,
    Comments nvarchar(150) NULL,
    BatchId uniqueidentifier NOT NULL,
    LoadDate datetime2 NOT NULL,
);
GO
