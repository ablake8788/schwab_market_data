
CREATE PROCEDURE dbo.usp_SchwabQuotesAnalytics_RunPortfolio
(
      @PortfolioID  INT
    , @StartDate    DATETIME2(3)  = NULL
    , @EndDate      DATETIME2(3)  = NULL

    -- analytics options
    , @ColumnName   NVARCHAR(50)  = N'QuoteLastPrice'  -- e.g. 'QuoteLastPrice' or 'QuoteNetPercentChange'
    , @NumStdDev    DECIMAL(5,2)  = 2.00
    , @ZThreshold   DECIMAL(5,2)  = 3.50

    -- batch grouping
    , @BatchId      UNIQUEIDENTIFIER = NULL
)
AS
/*==========================================================
Example usage:

-- Clean previous data (optional)
--DELETE dbo.SchwabQuotesAnalyticsBollingerBands;
--DELETE dbo.SchwabQuotesAnalyticsZScores;

DECLARE @BatchId UNIQUEIDENTIFIER = NEWID();

EXEC dbo.usp_SchwabQuotesAnalytics_RunPortfolio
      @PortfolioID = 1
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteLastPrice'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;

EXEC dbo.usp_SchwabQuotesAnalytics_RunPortfolio
      @PortfolioID = 1
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteNetPercentChange'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;   -- same batch to group both metrics
==========================================================*/
BEGIN
    SET NOCOUNT ON;

    --------------------------------------------------------------------
    -- 1. Ensure batch id for this portfolio run
    --------------------------------------------------------------------
    IF @BatchId IS NULL
        SET @BatchId = NEWID();

    --------------------------------------------------------------------
    -- 2. Prepare temp tables
    --------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#PortfolioSymbols') IS NOT NULL
        DROP TABLE #PortfolioSymbols;

    IF OBJECT_ID('tempdb..#Flagged') IS NOT NULL
        DROP TABLE #Flagged;

    --------------------------------------------------------------------
    -- 3. Load active symbols for portfolio
    --------------------------------------------------------------------
    SELECT DISTINCT
           CAST(Symbol AS NVARCHAR(20)) AS Symbol
    INTO #PortfolioSymbols
    FROM dbo.SchwabMarketDataPortfolioDefaultSymbol
    WHERE PortfolioID = @PortfolioID
      AND IsActive = 1;

    --------------------------------------------------------------------
    -- 4. Compute analytics for ALL portfolio symbols in one pass
    --------------------------------------------------------------------
    ;WITH Base AS
    (
        SELECT
              h.Symbol
            , h.BarDateTime AS QuoteDate
            , CASE @ColumnName
                  WHEN N'QuoteLastPrice'        THEN ISNULL(h.QuoteLastPrice,        0)
                  WHEN N'QuoteMark'             THEN ISNULL(h.QuoteMark,             0)
                  WHEN N'ClosePrice'            THEN ISNULL(h.ClosePrice,            0)
                  WHEN N'OpenPrice'             THEN ISNULL(h.OpenPrice,             0)
                  WHEN N'HighPrice'             THEN ISNULL(h.HighPrice,             0)
                  WHEN N'LowPrice'              THEN ISNULL(h.LowPrice,              0)
                  WHEN N'QuoteNetPercentChange' THEN ISNULL(h.QuoteNetPercentChange, 0)
                  ELSE ISNULL(h.QuoteLastPrice, 0)  -- default
              END AS RawValue
            , ISNULL(h.Quote52WeekHigh,            0) AS Quote52WeekHigh
            , ISNULL(h.Quote52WeekLow,             0) AS Quote52WeekLow
            , ISNULL(h.QuoteTotalVolume,           0) AS QuoteTotalVolume
            , ISNULL(h.FundamentalAvg10DaysVolume, 0) AS FundamentalAvg10DaysVolume
            , ISNULL(h.FundamentalAvg1YearVolume,  0) AS FundamentalAvg1YearVolume
            , ISNULL(h.QuoteNetPercentChange,      0) AS QuoteNetPercentChange
        FROM dbo.SchwabQuotesHistory_
