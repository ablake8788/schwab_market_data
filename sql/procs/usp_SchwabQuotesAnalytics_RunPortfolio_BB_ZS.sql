
CREATE PROCEDURE dbo.usp_SchwabQuotesAnalytics_RunPortfolio_BB_ZS
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
  3. EXECUTE: example runs

select * from dbo.SchwabQuotesAnalyticsBollingerBands
select * from dbo.SchwabQuotesAnalyticsZScores

delete dbo.SchwabQuotesAnalyticsBollingerBands
delete from dbo.SchwabQuotesAnalyticsZScores
----------------------------------



DECLARE @BatchId UNIQUEIDENTIFIER = NEWID();

EXEC dbo.usp_SchwabQuotesAnalytics_RunPortfolio_BB_ZS
      @PortfolioID = 1
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteLastPrice'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;

EXEC dbo.usp_SchwabQuotesAnalytics_RunPortfolio_BB_ZS
      @PortfolioID = 1
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteNetPercentChange'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;   -- same batch to group both metrics

==========================================================
*/

BEGIN
    SET NOCOUNT ON;

    --------------------------------------------------------------------
    -- 1. Ensure we have a batch id for this entire portfolio run
    --------------------------------------------------------------------
    IF @BatchId IS NULL
        SET @BatchId = NEWID();

    --------------------------------------------------------------------
    -- 2. Cursor over active symbols in the default portfolio table
    --------------------------------------------------------------------
    DECLARE @Symbol NVARCHAR(32);

    DECLARE curSymbols CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT
               Symbol
        FROM dbo.SchwabMarketDataPortfolioDefaultSymbol
        WHERE PortfolioID = @PortfolioID
          AND IsActive = 1;

    OPEN curSymbols;

    FETCH NEXT FROM curSymbols INTO @Symbol;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        ----------------------------------------------------------------
        -- 3. Call your existing analytics proc for each symbol
        ----------------------------------------------------------------
        EXEC dbo.usp_SchwabQuotesAnalytics_BB_ZS
              @Symbol      = @Symbol
            , @StartDate   = @StartDate
            , @EndDate     = @EndDate
            , @ColumnName  = @ColumnName
            , @NumStdDev   = @NumStdDev
            , @ZThreshold  = @ZThreshold
            , @BatchId     = @BatchId;

        FETCH NEXT FROM curSymbols INTO @Symbol;
    END

    CLOSE curSymbols;
    DEALLOCATE curSymbols;

    --------------------------------------------------------------------
    -- 4. Optionally return a combined view of what was just inserted
    --    (comment this out if you do not want a result set)
    --------------------------------------------------------------------
    SELECT
          bb.Symbol
        , bb.QuoteDate
        , bb.ColumnName
        , bb.WindowSize
        , bb.NumStdDev
        , bb.RawValue
        , bb.MiddleBand
        , bb.UpperBand
        , bb.LowerBand
        , zs.ZScore
        , zs.MetricType
        , zs.Scope
        , bb.BatchId
    FROM dbo.SchwabQuotesAnalyticsBollingerBands bb
    LEFT JOIN dbo.SchwabQuotesAnalyticsZScores zs
        ON  bb.Symbol    = zs.Symbol
        AND bb.QuoteDate = zs.QuoteDate
        AND bb.ColumnName = zs.ColumnName
        AND bb.BatchId   = zs.BatchId
    WHERE bb.BatchId = @BatchId
    ORDER BY bb.Symbol, bb.QuoteDate
