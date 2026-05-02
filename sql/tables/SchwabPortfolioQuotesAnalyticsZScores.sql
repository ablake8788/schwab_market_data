-- ============================================
-- Table: dbo.SchwabPortfolioQuotesAnalyticsZScores
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:02
-- ============================================

IF OBJECT_ID('dbo.SchwabPortfolioQuotesAnalyticsZScores', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabPortfolioQuotesAnalyticsZScores;
GO
CREATE TABLE dbo.SchwabPortfolioQuotesAnalyticsZScores (
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
