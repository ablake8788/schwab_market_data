-- ============================================
-- Table: dbo.SchwabMarketDataPortfolio
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:02
-- ============================================

IF OBJECT_ID('dbo.SchwabMarketDataPortfolio', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabMarketDataPortfolio;
GO
CREATE TABLE dbo.SchwabMarketDataPortfolio (
    PortfolioID int NOT NULL IDENTITY(1,1),
    PortfolioName nvarchar(100) NOT NULL,
    Description nvarchar(100) NOT NULL,
    IsActive int NOT NULL,
    CreatedOn datetime2 NOT NULL,
);
GO
