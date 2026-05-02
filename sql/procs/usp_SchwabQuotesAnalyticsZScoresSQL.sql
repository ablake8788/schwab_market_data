
CREATE   PROCEDURE dbo.usp_SchwabQuotesAnalyticsZScoresSQL
    @LookbackDays int          = 365,   -- days back from now to include
    @ZThreshold   decimal(10,4) = 3.0   -- |Z| threshold
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchId uniqueidentifier = NEWID();
    DECLARE @Now     datetime2(3)     = SYSUTCDATETIME();

    /*
      Columns included here:
        OpenPrice, HighPrice, LowPrice, ClosePrice,
        Volume,
        QuoteLastPrice, QuoteNetChange, QuoteNetPercentChange, QuoteTotalVolume

      All are explicitly cast to DECIMAL(18,8) so UNPIVOT can work without
      type conflicts.
    */

    ;WITH Base AS
    (
        SELECT
            h.Symbol,
            h.BarDateTime,

            -- Force a common DECIMAL(18,8) type for all metrics
            CAST(h.OpenPrice             AS decimal(18,8)) AS OpenPrice,
            CAST(h.HighPrice             AS decimal(18,8)) AS HighPrice,
            CAST(h.LowPrice              AS decimal(18,8)) AS LowPrice,
            CAST(h.ClosePrice            AS decimal(18,8)) AS ClosePrice,
            CAST(h.Volume                AS decimal(18,8)) AS Volume,
            CAST(h.QuoteLastPrice        AS decimal(18,8)) AS QuoteLastPrice,
            CAST(h.QuoteNetChange        AS decimal(18,8)) AS QuoteNetChange,
            CAST(h.QuoteNetPercentChange AS decimal(18,8)) AS QuoteNetPercentChange,
            CAST(h.QuoteTotalVolume      AS decimal(18,8)) AS QuoteTotalVolume
        FROM dbo.SchwabQuotesHistory_Summary            AS h
        INNER JOIN dbo.SchwabMarketDataPortfolioSymbol  AS p
            ON p.Symbol = h.Symbol
        WHERE h.BarDateTime >= DATEADD(DAY, -@LookbackDays, SYSUTCDATETIME())
    ),
    Unpvt AS
    (
        SELECT
            Symbol,
            BarDateTime,
            ColumnName,
            RawValue
        FROM Base
        UNPIVOT
        (
            RawValue FOR ColumnName IN
            (
                OpenPrice,
                HighPrice,
                LowPrice,
                ClosePrice,
                Volume,
                QuoteLastPrice,
                QuoteNetChange,
                QuoteNetPercentChange,
                QuoteTotalVolume
            )
        ) AS u
        WHERE RawValue IS NOT NULL
    ),
    Stats AS
    (
        SELECT
            Symbol,
            BarDateTime,
            ColumnName,
            RawValue,
            AVG(RawValue)  OVER (PARTITION BY Symbol, ColumnName) AS MeanValue,
            STDEVP(RawValue) OVER (PARTITION BY Symbol, ColumnName) AS StdDevValue
        FROM Unpvt
    ),
    Z AS
    (
        SELECT
            Symbol,
            BarDateTime,
            ColumnName,
            RawValue,
            CASE
                WHEN StdDevValue IS NULL OR StdDevValue = 0
                    THEN NULL    -- avoid divide-by-zero; no Z-score
                ELSE (RawValue - MeanValue) / StdDevValue
            END AS ZScore
        FROM Stats
    )
    INSERT INTO dbo.SchwabQuotesAnalyticsZScores
    (
        Symbol,
        QuoteDate,
        ColumnName,
        MetricType,
        Scope,
        RawValue,
        ZScore,
        OperationType,
        Comments,
        BatchId,
        LoadDate
    )
    SELECT
        Symbol,
        CONVERT(datetime2(3), BarDateTime)     AS QuoteDate,
        ColumnName,
        N'Z_GLOBAL'                            AS MetricType,
        N'BY_SYMBOL'                           AS Scope,
        RawValue,
        ZScore,
        N'HistorySummary Z'                    AS OperationType,
        N'Z_SCORE'                             AS Comments,
        @BatchId                               AS BatchId,
        @Now                                   AS LoadDate
    FROM Z
    WHERE ZScore IS NOT NULL
      AND ABS(ZScore) >= @ZThreshold;   -- anomaly threshold

END;

