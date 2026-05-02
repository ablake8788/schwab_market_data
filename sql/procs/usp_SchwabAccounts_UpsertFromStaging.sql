
CREATE   PROCEDURE dbo.usp_SchwabAccounts_UpsertFromStaging
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH SourceCTE AS
    (
        SELECT
            s.AccountNumber,
            MAX(s.AccountType)    AS AccountType,
            MAX(s.Nickname)       AS Nickname,
            MAX(s.Status)         AS Status,
            MAX(s.IsMargin)       AS IsMargin,
            MAX(s.OperationType)  AS OperationType,
            MAX(s.Comments)       AS Comments,
            MAX(s.BatchId)        AS BatchId,        -- UNIQUEIDENTIFIER in staging
            MAX(s.LoadDate)       AS LoadDate,
            MAX(s.CreatedBy)      AS CreatedBy,
            MAX(s.CreatedOn)      AS CreatedOn,
            MAX(s.ExtractedAt)    AS LatestExtractedAt
        FROM dbo.SchwabAccountsStaging s
        GROUP BY
            s.AccountNumber
    )
    MERGE dbo.SchwabAccounts AS target
    USING SourceCTE AS src
        ON target.AccountNumber = src.AccountNumber
    WHEN MATCHED THEN
        UPDATE SET
            target.AccountType   = src.AccountType,
            target.Nickname      = src.Nickname,
            target.Status        = src.Status,
            target.IsMargin      = src.IsMargin,
            target.OperationType = ISNULL(src.OperationType, 'API ETL'),
            target.Comments      = src.Comments,
            target.BatchId       = src.BatchId,
            target.LoadDate      = ISNULL(src.LoadDate,  SYSDATETIME()),
            target.LastEditBy    = ISNULL(src.CreatedBy, SUSER_SNAME()),
            target.LastEditOn    = ISNULL(src.CreatedOn, SYSDATETIME())
    WHEN NOT MATCHED BY TARGET THEN
        INSERT
        (
            AccountNumber,
            AccountType,
            Nickname,
            Status,
            IsMargin,
            OperationType,
            Comments,
            BatchId,
            LoadDate,
            CreatedBy,
            CreatedOn,
            LastEditBy,
            LastEditOn
        )
        VALUES
        (
            src.AccountNumber,
            src.AccountType,
            src.Nickname,
            src.Status,
            src.IsMargin,
            ISNULL(src.OperationType, 'API ETL'),
            src.Comments,
            src.BatchId,
            ISNULL(src.LoadDate,  SYSDATETIME()),
            ISNULL(src.CreatedBy, SUSER_SNAME()),   -- NOT NULL
            ISNULL(src.CreatedOn, SYSDATETIME()),   -- NOT NULL
            ISNULL(src.CreatedBy, SUSER_SNAME()),
            ISNULL(src.CreatedOn, SYSDATETIME())
        );

END;

