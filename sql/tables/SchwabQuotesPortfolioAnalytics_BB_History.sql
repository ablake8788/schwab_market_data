-- ============================================
-- Table: dbo.SchwabQuotesPortfolioAnalytics_BB_History
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:05
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesPortfolioAnalytics_BB_History', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesPortfolioAnalytics_BB_History;
GO
CREATE TABLE dbo.SchwabQuotesPortfolioAnalytics_BB_History (
    Id int NOT NULL,
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
    BandWidthAbs decimal(18,2) NULL,
    BandWidthPct decimal(18,2) NULL,
    WindowCount int NULL,
    IsWindowReady bit NULL,
    RollStd decimal(18,6) NULL,
    BollingerZ decimal(18,2) NULL,
    GlobalMedian decimal(18,6) NULL,
    GlobalMAD decimal(18,6) NULL,
    RobustZ decimal(18,2) NULL,
    ZFlag bit NULL,
    RiskScore decimal(18,6) NULL,
    SysStartTime datetime2 NOT NULL,
    SysEndTime datetime2 NOT NULL,
);
GO
