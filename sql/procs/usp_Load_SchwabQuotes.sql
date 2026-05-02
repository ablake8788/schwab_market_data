
CREATE   PROCEDURE dbo.usp_Load_SchwabQuotes
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
     BEGIN TRAN;

        EXEC dbo.usp_Load_SchwabQuotes_Stage;
        EXEC dbo.usp_Load_SchwabQuotes_Stage_to_SchwabQuotes;

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
        THROW;
    END CATCH;
END;

