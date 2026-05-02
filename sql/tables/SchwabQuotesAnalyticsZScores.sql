-- ============================================
-- Table: dbo.SchwabQuotesAnalyticsZScores
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:04
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesAnalyticsZScores', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesAnalyticsZScores;
GO
CREATE TABLE dbo.SchwabQuotesAnalyticsZScores (
    Id int NOT NULL IDENTITY(1,1),
    Symbol nvarchar(20) NOT NULL,
    QuoteDate datetime2 NOT NULL,
    ColumnName nvarchar(50) NOT NULL,
    MetricType nvarchar(30) NOT NULL,
    Scope nvarchar(30) NOT NULL,
    RawValue decimal(18,8) NOT NULL,
    ZScore decimal(18,8) NOT NULL,
    OperationType nvarchar(150) NULL,
    Comments nvarchar(150) NULL,
    BatchId uniqueidentifier NOT NULL,
    LoadDate datetime2 NOT NULL,
);
GO
