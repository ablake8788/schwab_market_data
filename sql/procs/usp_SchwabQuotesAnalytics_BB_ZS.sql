
CREATE PROCEDURE dbo.usp_SchwabQuotesAnalytics_BB_ZS
(
    @Symbol        NVARCHAR(20)  = NULL,
    @StartDate     DATETIME2(3)  = NULL,
    @EndDate       DATETIME2(3)  = NULL,

    @ColumnName    NVARCHAR(50)  = N'QuoteLastPrice', -- which column to analyze
    @NumStdDev     DECIMAL(5,2)  = 2.00,              -- Bollinger Nσ
    @ZThreshold    DECIMAL(5,2)  = 3.50,              -- Robust Z anomaly threshold

    @BatchId       UNIQUEIDENTIFIER = NULL
)
AS
/*==========================================================
  3. EXECUTE: example runs

select * from dbo.SchwabQuotesAnalyticsBollingerBands
select * from dbo.SchwabQuotesAnalyticsZScores

delete dbo.SchwabQuotesAnalyticsBollingerBands
delete from dbo.SchwabQuotesAnalyticsZScores


DECLARE @BatchId UNIQUEIDENTIFIER = NEWID();

-- Example 1: Last price analytics for one symbol
EXEC dbo.usp_SchwabQuotesAnalytics_BB_ZS
      @Symbol      = N'RR'          -- change to a symbol that exists in your data
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteLastPrice'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;

-- Example 2: Net percent change analytics for same batch
EXEC dbo.usp_SchwabQuotesAnalytics_BB_ZS
      @Symbol      = N''
    , @StartDate   = '2024-01-01'
    , @EndDate     = NULL
    , @ColumnName  = N'QuoteNetPercentChange'
    , @NumStdDev   = 2.0
    , @ZThreshold  = 3.5
    , @BatchId     = @BatchId;

==========================================================
*/

BEGIN
    SET NOCOUNT ON;

    IF @BatchId IS NULL
        SET @BatchId = NEWID();

    IF OBJECT_ID('tempdb..#Flagged') IS NOT NULL
        DROP TABLE #Flagged;

    --------------------------------------------------------------------
    -- 1) Build base + rolling stats, materialize into #Flagged
    --------------------------------------------------------------------
    ;WITH Base AS
    (
        SELECT
              h.Symbol
            , h.BarDateTime                               AS QuoteDate
            , CASE @ColumnName
                  WHEN N'QuoteLastPrice'        THEN h.QuoteLastPrice
                  WHEN N'QuoteMark'             THEN h.QuoteMark
                  WHEN N'ClosePrice'            THEN h.ClosePrice
                  WHEN N'OpenPrice'             THEN h.OpenPrice
                  WHEN N'HighPrice'             THEN h.HighPrice
                  WHEN N'LowPrice'              THEN h.LowPrice
                  WHEN N'QuoteNetPercentChange' THEN h.QuoteNetPercentChange
                  ELSE h.QuoteLastPrice  -- default
              END                                         AS RawValue
            , h.Quote52WeekHigh
            , h.Quote52WeekLow
            , h.QuoteTotalVolume
            , h.FundamentalAvg10DaysVolume
            , h.FundamentalAvg1YearVolume
            , h.QuoteNetPercentChange
        FROM dbo.SchwabQuotesHistory_Summary h
        WHERE (@Symbol    IS NULL OR h.Symbol = @Symbol)
          AND (@StartDate IS NULL OR h.BarDateTime >= @StartDate)
          AND (@EndDate   IS NULL OR h.BarDateTime <= @EndDate)
    ),
    Stats AS
    (
        SELECT
              b.*
            , AVG(RawValue) OVER
                (
                    PARTITION BY b.Symbol
                    ORDER BY b.QuoteDate
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW   -- 20-bar window
                ) AS roll_avg
            , STDEV(RawValue) OVER
                (
                    PARTITION BY b.Symbol
                    ORDER BY b.QuoteDate
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW   -- 20-bar window
                ) AS roll_std
            , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY RawValue)
                OVER (PARTITION BY b.Symbol) AS roll_med
            , ABS(
                  RawValue
                - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
