-- ============================================
-- Table: dbo.SchwabMarketDataPortfolioSymbol
-- Database: PBI_Projects
-- Generated: 2026-05-01 15:21:02
-- ============================================

IF OBJECT_ID('dbo.SchwabMarketDataPortfolioSymbol', 'U') IS NOT NULL
    DROP TABLE dbo.SchwabMarketDataPortfolioSymbol;
GO
CREATE TABLE dbo.SchwabMarketDataPortfolioSymbol (
    PortfolioID int NOT NULL,
    Symbol varchar(32) NOT NULL,
    Quantity decimal(18,4) NULL,
    IsActive bit NOT NULL,
    LastUpdated datetime2 NOT NULL,
);
GO
