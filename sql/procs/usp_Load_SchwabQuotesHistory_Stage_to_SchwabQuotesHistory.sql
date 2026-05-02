
CREATE   PROCEDURE dbo.usp_Load_SchwabQuotesHistory_Stage_to_SchwabQuotesHistory
                         
AS
/*
EXEC dbo.usp_Load_SchwabQuotesHistory_Stage_to_SchwabQuotesHistory;
select count(*) from SchwabQuotesHistory
*/
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchId NVARCHAR(50) = CONVERT(NVARCHAR(50), NEWID());
    DECLARE @User    NVARCHAR(50) = SUSER_SNAME();

    /*
      Insert only new bars (Symbol + BarDateTime) into the temporal table.
      SysStartTime / SysEndTime are handled automatically by SQL Server.
    */
    INSERT INTO dbo.SchwabQuotesHistory
    (
        RawId,
        Symbol,
        BarDateTime,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        Volume,
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
        s.RawId,
        s.Symbol,
        s.BarDateTime,
        s.OpenPrice,
        s.HighPrice,
        s.LowPrice,
        s.ClosePrice,
        s.Volume,
        s.InsertedAt,
        -- metadata defaults
        'SCHWAB API ETL'          AS OperationType,
        'SCHWAB API ETL STAGE TO'               AS Comments,
        @BatchId           AS BatchId,
        SYSUTCDATETIME()   AS LoadDate,
        @User              AS CreatedBy,
        SYSUTCDATETIME()   AS CreatedOn,
        NULL               AS LastEditBy,
        NULL               AS LastEditOn
    FROM dbo.SchwabQuotesHistory_Stage s
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.SchwabQuotesHistory h
        WHERE h.Symbol      = s.Symbol
          AND h.BarDateTime = s.BarDateTime
    );
END;

