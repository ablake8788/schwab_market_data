
CREATE PROCEDURE dbo.usp_SchwabQuotesRunPortfolioAnalytics_BB_ZS
(
      @PortfolioID  INT
    , @StartDate    DATETIME2(3)  = NULL
    , @EndDate      DATETIME2(3)  = NULL

    -- NOTE: ColumnName is still stored/logged, but RawValue is computed from ClosePrice
    , @ColumnName   NVARCHAR(50)  = N'ClosePrice'
    , @Window       INT           = 20
    , @NumStdDev    DECIMAL(5,2)  = 2.00
    , @ZThreshold   DECIMAL(5,2)  = 3.50

    , @BatchId      UNIQUEIDENTIFIER = NULL
)
AS

/*
DECLARE @BatchId uniqueidentifier = NEWID();

EXEC dbo.usp_SchwabQuotesRunPortfolioAnalytics_BB_ZS
      @PortfolioID = 1
    , @StartDate   = '2025-10-01'
    , @EndDate     = NULL
    , @ColumnName  = N'ClosePrice'
    , @Window      = 25
    , @NumStdDev   = 2.00
    , @ZThreshold  = 1.00
    , @BatchId     = @BatchId;

SELECT @BatchId AS BatchId;

--DECLARE @BatchId uniqueidentifier = NEWID();
EXEC dbo.usp_SchwabQuotesRunPortfolioAnalytics_BB_ZS
      @PortfolioID = 1
    , @StartDate   = '2025-10-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteLastPrice'
    , @Window      = 25
    , @NumStdDev   = 2.00
    , @ZThreshold  = 1.00
    , @BatchId     = @BatchId;

SELECT @BatchId AS BatchId;

--=================



SELECT TOP (20)
    Symbol, QuoteDate, RawValue, MiddleBand, UpperBand, LowerBand, RollStd, NumStdDev,
    (MiddleBand + NumStdDev * RollStd) AS CalcUpper,
    (MiddleBand - NumStdDev * RollStd) AS CalcLower
FROM dbo.SchwabQuotesPortfolioAnalytics_BB
WHERE ColumnName = 'ClosePrice'
ORDER BY QuoteDate DESC;


SELECT
    Symbol,
    CAST(QuoteDate AS date) AS TradeDate,
    COUNT(*) AS RowsPerDay,
    MIN(QuoteDate) AS MinDT,
    MAX(QuoteDate) AS MaxDT
FROM dbo.SchwabQuotesPortfolioAnalytics_BB
WHERE ColumnName = 'ClosePrice'
  AND QuoteDate >= '2024-12-01'
GROUP BY Symbol, CAST(QuoteDate AS date)
ORDER BY Symbol,TradeDate DESC;

 select distinct symbol from  dbo.SchwabQuotesHistory_Summary


select * from dbo.SchwabQuotesPortfolioAnalytics_BB
select * from dbo.SchwabPortfolioQuotesAnalyticsZScores

delete  dbo.SchwabQuotesPortfolioAnalytics_BB
delete  dbo.SchwabPortfolioQuotesAnalyticsZScores

--
select * from dbo.SchwabMarketDataPortfolio
select * from dbo.SchwabMarketDataPortfolioSymbol
select * from SchwabQuotes_Raw
select * from SchwabQuotes_Stage
select * from SchwabQuotes

----delete  dbo.SchwabMarketDataPortfolio
----delete  dbo.SchwabMarketDataPortfolioSymbol

------delete  SchwabQuotes_Raw
------delete  SchwabQuotes_Stage
------delete  SchwabQuotes
*/

BEGIN
    SET NOCOUNT ON;

    IF @BatchId IS NULL SET @BatchId = NEWID();
    IF @Window IS NULL OR @Window < 2 SET @Window = 20;

    IF OBJECT_ID('tempdb..#PortfolioSymbols') IS NOT NULL DROP TABLE #PortfolioSymbols;
    IF OBJECT_ID('tempdb..#Flagged')          IS NOT NULL DROP TABLE #Flagged;

    SELECT DISTINCT CAST(Symbol AS NVARCHAR(20)) AS Symbol
    INTO #PortfolioSymbols
    FROM dbo.SchwabMarketDataPortfolioDefaultSymbol
    WHERE PortfolioID = @PortfolioID
      AND IsActive = 1;

    CREATE TABLE #Flagged
    (
        Symbol NVARCHAR(20) NOT NULL,
        QuoteDate DATETIME2(3) NOT NULL,
        ColumnName NVARCHAR(50) NOT NULL,

        RawValueRaw DECIMAL(18,8) NULL,
        Quote52WeekHigh DECIMAL(18,2) NULL,
        Quote52WeekLow  DECIMAL(18,2) NULL,
        QuoteTotalVolume BIGINT NULL,
        FundamentalAvg10DaysVolume DECIMAL(18,2) NULL,
        FundamentalAvg1YearVolume  DECIMAL(18,2) NULL,
        QuoteNetPercentChange DECIMAL(18,4) NULL,

        WindowCount INT NULL,
        IsWindowReady BIT NULL,

        MiddleBand DECIMAL(18,8) NULL,
        RollStd    DECIMAL(18,8) NULL,
        UpperBand  DECIMAL(18,8) NULL,
        LowerBand  DECIMAL(18,8) NULL,

        BandWidthAbs DECIMAL(18,8) NULL,
        BandWidthPct DECIMAL(18,8) NULL,

        BollingerZ DECIMAL(18,8) NULL,

        GlobalMedian DECIMAL(18,8
