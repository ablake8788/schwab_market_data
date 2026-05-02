
CREATE FUNCTION dbo.fn_ComputeQuoteRiskScore
(
    @Price              DECIMAL(18,4),
    @High52Week         DECIMAL(18,4),
    @Low52Week          DECIMAL(18,4),
    @Volume             BIGINT,
    @Avg10DayVolume     DECIMAL(18,4),
    @Avg1YearVolume     DECIMAL(18,4),
    @BollingerZ         DECIMAL(18,8),
    @RobustZ            DECIMAL(18,8),
    @NetPercentChange   DECIMAL(18,8)
)
RETURNS INT
AS
/*
    dbo.fn_ComputeQuoteRiskScore

    High-level idea:
      - Price near 52-week extremes  -> risk
      - Large % move intraday        -> risk
      - Volume spikes vs averages    -> risk
      - Large Bollinger Z / Robust Z -> risk
*/
BEGIN
    DECLARE @RiskScore INT = 0;

    --------------------------------------------------------------------
    -- Price vs 52-week extremes
    --------------------------------------------------------------------
    IF @High52Week IS NOT NULL AND @High52Week > 0 AND @Price >= @High52Week * 0.98
        SET @RiskScore += 10;  -- near 52-week high

    IF @Low52Week IS NOT NULL AND @Low52Week > 0 AND @Price <= @Low52Week * 1.02
        SET @RiskScore += 10;  -- near 52-week low

    --------------------------------------------------------------------
    -- Intraday percent move
    --------------------------------------------------------------------
    IF @NetPercentChange IS NOT NULL
    BEGIN
        -- Large down move
        IF @NetPercentChange <= -0.05       -- -5% or worse
            SET @RiskScore += 20;
        ELSE IF @NetPercentChange <= -0.03  -- -3% to -5%
            SET @RiskScore += 10;

        -- Large up move (less penalized but still significant)
        IF @NetPercentChange >= 0.05        -- +5% or more
            SET @RiskScore += 10;
    END

    --------------------------------------------------------------------
    -- Volume spikes vs rolling averages
    --------------------------------------------------------------------
    IF @Avg10DayVolume IS NOT NULL AND @Avg10DayVolume > 0
    BEGIN
        IF @Volume >= 2 * @Avg10DayVolume
            SET @RiskScore += 10;  -- 2x 10D avg
        IF @Volume >= 4 * @Avg10DayVolume
            SET @RiskScore += 5;   -- 4x 10D avg
    END

    IF @Avg1YearVolume IS NOT NULL AND @Avg1YearVolume > 0
    BEGIN
        IF @Volume >= 3 * @Avg1YearVolume
            SET @RiskScore += 10;  -- 3x 1Y avg
    END

    --------------------------------------------------------------------
    -- Bollinger band excursion (standardized distance from middle band)
    --------------------------------------------------------------------
    IF @BollingerZ IS NOT NULL
    BEGIN
        IF @BollingerZ >= 2.0 OR @BollingerZ <= -2.0
            SET @RiskScore += 10;  -- out of ±2σ
        IF @BollingerZ >= 3.0 OR @BollingerZ <= -3.0
            SET @RiskScore += 10;  -- out of ±3σ
    END

    --------------------------------------------------------------------
    -- Robust Z-score (MAD-based)
    --------------------------------------------------------------------
    IF @RobustZ IS NOT NULL
    BEGIN
        IF ABS(@RobustZ) BETWEEN 2.0 AND 3.0
            SET @RiskScore += 10;

        IF ABS(@RobustZ) > 3.0
            SET @RiskScore += 20;
    END

    RETURN @RiskScore;
END;

