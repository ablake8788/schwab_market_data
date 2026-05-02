
CREATE   PROCEDURE dbo.usp_Load_SchwabQuotesHistory_Summary
AS
BEGIN
    SET NOCOUNT ON;


    DECLARE @BatchId NVARCHAR(50) = CONVERT(NVARCHAR(50), NEWID());
    DECLARE @User    NVARCHAR(50) = SUSER_SNAME();

    -- Optional "full refresh" pattern; if you prefer incremental, change this.
    DELETE dbo.SchwabQuotesHistory_Summary;

    ;WITH LatestQuotes AS
    (
        SELECT q.*
        FROM dbo.SchwabQuotes_Stage q
        INNER JOIN
        (
            SELECT Symbol, MAX(QuoteQuoteTime) AS MaxQuoteQuoteTime
            FROM dbo.SchwabQuotes_Stage
            GROUP BY Symbol
        ) mx
            ON q.Symbol = mx.Symbol
           AND q.QuoteQuoteTime = mx.MaxQuoteQuoteTime
    )
    INSERT INTO dbo.SchwabQuotesHistory_Summary
    (
        Symbol,
        BarDateTime,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        Volume,
        FundamentalsAsOf,
        FundamentalAvg10DaysVolume,
        FundamentalAvg1YearVolume,
        FundamentalDivAmount,
        FundamentalDivFreq,
        FundamentalDivPayAmount,
        FundamentalDivYield,
        FundamentalEps,
        FundamentalFundLeverageFactor,
        FundamentalLastEarningsDate,
        FundamentalPeRatio,
        Quote52WeekHigh,
        Quote52WeekLow,
        QuoteLastPrice,
        QuoteMark,
        QuoteNetChange,
        QuoteNetPercentChange,
        QuoteTotalVolume,
         -- metadata defaults
        InsertedAt,
        OperationType,
        Comments,
        BatchId,
        LoadDate,
        CreatedBy,
        CreatedOn,
        LastEditBy,
        LastEditOn
    )
    SELECT
        h.Symbol,
        h.BarDateTime,
        h.OpenPrice,
        h.HighPrice,
        h.LowPrice,
        h.ClosePrice,
        h.Volume,
        --Fundamentals
        q.QuoteQuoteTime           AS FundamentalsAsOf,   -- BIGINT epoch ms; convert if you prefer
        q.FundamentalAvg10DaysVolume,
        q.FundamentalAvg1YearVolume,
        q.FundamentalDivAmount,
        q.FundamentalDivFreq,
        q.FundamentalDivPayAmount,
        q.FundamentalDivYield,
        q.FundamentalEps,
        q.FundamentalFundLeverageFactor,
        q.FundamentalLastEarningsDate,
        q.FundamentalPeRatio,
        q.Quote52WeekHigh,
        q.Quote52WeekLow,
        q.QuoteLastPrice,
        q.QuoteMark,
        q.QuoteNetChange,
        q.QuoteNetPercentChange,
        q.QuoteTotalVolume,
         -- metadata defaults
         q.InsertedAt,
        'SCHWAB API FUNDAMENTALS SUM' AS OperationType,
        'SCHWAB FUNDAMENTALS SchwabQuotesHistory_Summary '               AS Comments,
        @BatchId           AS BatchId,
        SYSUTCDATETIME()   AS LoadDate,
        @User              AS CreatedBy,
        SYSUTCDATETIME()   AS CreatedOn,
        NULL               AS LastEditBy,
        NULL               AS LastEditOn
    FROM dbo.SchwabQuotesHistory_Stage h
    LEFT JOIN LatestQuotes q
        ON h.Symbol = q.Symbol;
END;

