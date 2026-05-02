
CREATE   PROCEDURE dbo.usp_Load_SchwabQuotes_Stage
AS
/*
EXECUTE dbo.usp_Load_SchwabQuotes_Stage
--
SELECT TOP (1)
       JSON_VALUE(r.RawJson, '$.quote."52WeekHigh"') AS High52,
       JSON_VALUE(r.RawJson, '$.quote."52WeekLow"')  AS Low52
FROM dbo.SchwabQuotes_Raw r
WHERE ISJSON(r.RawJson) = 1
*/
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.SchwabQuotes_Stage
    (
        RawId,
        AssetMainType,
        AssetSubType,
        QuoteType,
        Realtime,
        Ssid,
        Symbol,
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
        QuoteAskMICId,
        QuoteAskPrice,
        QuoteAskSize,
        QuoteAskTime,
        QuoteBidMICId,
        QuoteBidPrice,
        QuoteBidSize,
        QuoteBidTime,
        QuoteClosePrice,
        QuoteHighPrice,
        QuoteLastMICId,
        QuoteLastPrice,
        QuoteLastSize,
        QuoteLowPrice,
        QuoteMark,
        QuoteMarkChange,
        QuoteMarkPercentChange,
        QuoteNetChange,
        QuoteNetPercentChange,
        QuoteOpenPrice,
        QuotePostMarketChange,
        QuotePostMarketPercentChange,
        QuoteQuoteTime,
        QuoteSecurityStatus,
        QuoteTotalVolume,
        QuoteTradeTime
    )
    SELECT
        r.Id AS RawId,
        j.AssetMainType,
        j.AssetSubType,
        j.QuoteType,
        j.Realtime,
        j.Ssid,
        j.Symbol,
        j.FundamentalAvg10DaysVolume,
        j.FundamentalAvg1YearVolume,
        j.FundamentalDivAmount,
        j.FundamentalDivFreq,
        j.FundamentalDivPayAmount,
        j.FundamentalDivYield,
        j.FundamentalEps,
        j.FundamentalFundLeverageFactor,
        j.FundamentalLastEarningsDate,
        j.FundamentalPeRatio,
        j.Quote52WeekHigh,
        j.Quote52WeekLow,
        j.QuoteAskMICId,
        j.QuoteAskPrice,
        j.QuoteAskSize,
        j.QuoteAskTime,
        j.QuoteBidMICId,
        j.QuoteBidPrice,
        j.QuoteBidSize,
        j.QuoteBidTime,
        j.QuoteClosePrice,
        j.QuoteHighPrice,
        j.QuoteLastMICId,
        j.QuoteLastPrice,
        j.QuoteLastSize,
        j.QuoteLowPrice,
        j.QuoteMark,
        j.QuoteMarkChange,
        j.QuoteMarkPercentChange,
        j.QuoteNetChange,
        j.QuoteNetPercentChange,
        j.QuoteOpenPrice,
        j.QuotePostMarketChange,
        j.QuotePostMarketPercentChange,
        j.QuoteQuoteTime,
        j.QuoteSecurityStatus,
        j.QuoteTotalVolume,
        j.QuoteTradeTime
    FROM dbo.SchwabQuotes_Raw r
       CROSS APPLY OPENJSON(r.RawJson)
       WITH
    (
        -- top level
        AssetMainType   NVARCHAR(20)   '$.assetMainType',
        AssetSubType    NVARCHAR(20)   '$.assetSubType',
        QuoteType       NVARCHAR(20)   '$.quoteType',
        Realtime        BIT            '$.realtime',
        Ssid            BIGINT         '$.ssid',
        Symbol          NVARCHAR(20)   '$.symbol',

        -- fundamental.*
        FundamentalAvg10DaysVolume      DECIMAL(18,4) '$.fundamental.avg10DaysVolume',
        FundamentalAvg1YearVolume       DECIMAL(18,4) '$.fundamental.avg1YearVolume',
        FundamentalDivAmount            DECIMAL(18,4) '$.fundamental.divAmount',
        FundamentalDivFreq              INT           '$.fundamental.divFreq',
        FundamentalDivPayAmount         DECIMAL(18,4) '$.fundamental.divPayAmount',
        FundamentalDivYield             DECIMAL(18,6) '$.fundamental.divYield',
        FundamentalEps                  DECIMAL(18,6) '$.fundamental.eps',
        FundamentalFundLeverageFactor   DECIMAL(18,6) '$.fundamenta
