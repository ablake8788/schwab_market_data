
CREATE   PROCEDURE dbo.usp_Load_SchwabQuotesHistory_Stage
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.SchwabQuotesHistory_Stage
    (
        RawId,
        Symbol,
        BarDateTime,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        Volume,
        RawJson
    )
    SELECT
        r.Id            AS RawId,
        r.Symbol,
        r.BarDateTime,
        r.OpenPrice,
        r.HighPrice,
        r.LowPrice,
        r.ClosePrice,
        r.Volume,
        r.RawJson
    FROM dbo.SchwabQuotesHistory_Raw AS r
    WHERE
        r.RawJson IS NOT NULL
        AND ISJSON(r.RawJson) = 1
        -- avoid loading same RawId multiple times
        AND NOT EXISTS
        (
            SELECT 1
            FROM dbo.SchwabQuotesHistory_Stage s
            WHERE s.RawId = r.Id
        );
END;

