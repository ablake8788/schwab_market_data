CREATE   PROCEDURE dbo.usp_Load_SchwabQuotesHistory
AS
BEGIN
    SET NOCOUNT ON;

   EXEC dbo.usp_Load_SchwabQuotesHistory_Stage;
    --========
   EXEC dbo.usp_Load_SchwabQuotesHistory_Stage_to_SchwabQuotesHistory;

    -- Optional: move from Stage -> final history table, if you have one:
    /*
    INSERT INTO dbo.SchwabQuotesHistory (Symbol, BarDateTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
    SELECT Symbol, BarDateTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume
    FROM dbo.SchwabQuotesHistory_Stage s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.SchwabQuotesHistory f
        WHERE f.Symbol = s.Symbol
          AND f.BarDateTime = s.BarDateTime
    );

    select * from SchwabQuotesHistory_Raw
select * from SchwabQuotesHistory_Stage
select * from SchwabQuotesHistory
select distinct symbol from SchwabQuotesHistory
select symbol, count(*)  from SchwabQuotesHistory
group by symbol

symbol	(No column name)
ACHR	12802
BBAI	12827
LUNR	12715
QBTS	12798
RGTI	12800
RR	12779
WULF	12805

    */


END;

