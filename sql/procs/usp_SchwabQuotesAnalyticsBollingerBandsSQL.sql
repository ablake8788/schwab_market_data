
CREATE   PROCEDURE dbo.usp_SchwabQuotesAnalyticsBollingerBandsSQL
    @LookbackDays int = 365,   -- days back from now to include
    @Window       int = 20,    -- Bollinger window size
    @K            int = 2      -- number of std devs
AS
/*
--delete SchwabQuotesAnalyticsBollingerBandsSQL
EXEC dbo.usp_SchwabQuotesAnalyticsBollingerBandsSQL;
-- or with overrides:
EXEC dbo.usp_SchwabQuotesAnalyticsBollingerBandsSQL
    @LookbackDays = 180,
    @Window       = 20,
    @K            = 2;

select * from dbo.SchwabQuotesAnalyticsBollingerBandsSQL
*/
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchId uniqueidentifier = NEWID();
    DECLARE @Now     datetime2(3)     = SYSUTCDATETIME();

    /*
      NOTE: the window frame below uses 19 PRECEDING (i.e., 20-bar window).
            If you change @Window, you must also update the frame literal
            to (@Window - 1) manually or switch to dynamic SQL.
    */

    ;WITH Prices AS
    (
        SELECT h.*
        FROM dbo.SchwabQuotesHistory_Summary            AS h
        INNER JOIN dbo.SchwabMarketDataPortfolioSymbol  AS p
            ON p.Symbol = h.Symbol
        WHERE h.BarDateTime >= DATEADD(DAY, -@LookbackDays, SYSUTCDATETIME())
    ),
    BB AS
    (
        SELECT
            p.Symbol,
            p.BarDateTime,
            p.ClosePrice AS RawValue,

            AVG(p.ClosePrice) OVER (
                PARTITION BY p.Symbol
                ORDER BY p.BarDateTime
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW  -- @Window - 1
            ) AS MiddleBand,

            STDEVP(p.ClosePrice) OVER (
                PARTITION BY p.Symbol
                ORDER BY p.BarDateTime
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW  -- @Window - 1
            ) AS StdDev
        FROM Prices AS p
    )
    INSERT INTO dbo.SchwabQuotesAnalyticsBollingerBandsSQL
    (
        Symbol,
        QuoteDate,
        ColumnName,
        WindowSize,
        NumStdDev,
        RawValue,
        MiddleBand,
        UpperBand,
        LowerBand,
        OperationType,
        Comments,
        BatchId,
        LoadDate
    )
    SELECT
        Symbol,
        CONVERT(datetime2(3), BarDateTime)                         AS QuoteDate,
        N'ClosePrice'                                              AS ColumnName,  -- source column
        @Window                                                     AS WindowSize,
        @K                                                          AS NumStdDev,
        CAST(RawValue                 AS decimal(18,8))             AS RawValue,
        CAST(MiddleBand               AS decimal(18,8))             AS MiddleBand,
        CAST(MiddleBand + @K * StdDev AS decimal(18,8))             AS UpperBand,
        CAST(MiddleBand - @K * StdDev AS decimal(18,8))             AS LowerBand,
        N'HistorySummary BB'                                        AS OperationType,
        N'Bollinger from SQL'                                      AS Comments,
        @BatchId                                                    AS BatchId,
        @Now                                                        AS LoadDate
    FROM BB
    WHERE MiddleBand IS NOT NULL;
END;

