-- ============================================
-- Table: dbo.SchwabQuotesHistory_Summary_History
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:05
-- ============================================

IF OBJECT_ID('dbo.SchwabQuotesHistory_Summary_History', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabQuotesHistory_Summary_History;
GO
CREATE TABLE dbo.SchwabQuotesHistory_Summary_History (
    SummaryId int NOT NULL,
    Symbol nvarchar(20) NOT NULL,
    BarDateTime datetime2 NOT NULL,
    OpenPrice decimal(18,4) NULL,
    HighPrice decimal(18,4) NULL,
    LowPrice decimal(18,4) NULL,
    ClosePrice decimal(18,4) NULL,
    Volume bigint NULL,
    FundamentalsAsOf bigint NULL,
    FundamentalAvg10DaysVolume decimal(18,4) NULL,
    FundamentalAvg1YearVolume decimal(18,4) NULL,
    FundamentalDivAmount decimal(18,4) NULL,
    FundamentalDivFreq int NULL,
    FundamentalDivPayAmount decimal(18,4) NULL,
    FundamentalDivYield decimal(18,6) NULL,
    FundamentalEps decimal(18,6) NULL,
    FundamentalFundLeverageFactor decimal(18,6) NULL,
    FundamentalLastEarningsDate datetime2 NULL,
    FundamentalPeRatio decimal(18,6) NULL,
    Quote52WeekHigh decimal(18,4) NULL,
    Quote52WeekLow decimal(18,4) NULL,
    QuoteLastPrice decimal(18,4) NULL,
    QuoteMark decimal(18,4) NULL,
    QuoteNetChange decimal(18,4) NULL,
    QuoteNetPercentChange decimal(18,8) NULL,
    QuoteTotalVolume bigint NULL,
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
