-- Advanced Technical Analysis and Pattern Recognition
-- This query demonstrates complex window functions, recursive CTEs, and custom calculations

-- First, create a function for calculating RSI
 //
CREATE FUNCTION calculate_rsi(
    symbol VARCHAR(10),
    period INT,
    start_date DATE,
    end_date DATE
) RETURNS DECIMAL(5,2)
DETERMINISTIC
BEGIN
    DECLARE avg_gain DECIMAL(10,2);
    DECLARE avg_loss DECIMAL(10,2);
    
    -- Calculate average gains and losses
    SELECT 
        AVG(CASE WHEN price_change > 0 THEN price_change ELSE 0 END),
        AVG(CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END)
    INTO avg_gain, avg_loss
    FROM (
        SELECT 
            close_price - LAG(close_price) OVER (ORDER BY date) as price_change
        FROM stock_prices
        WHERE symbol = symbol
        AND date BETWEEN start_date AND end_date
    ) price_changes;
    
    -- Calculate RSI
    RETURN 100 - (100 / (1 + (avg_gain / avg_loss)));
END //
 ;

-- Main analysis query
WITH price_changes AS (
    -- Calculate daily price changes and returns
    SELECT 
        symbol,
        date,
        close_price,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
        (close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY date)) / 
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) as daily_return,
        volume,
        LAG(volume) OVER (PARTITION BY symbol ORDER BY date) as prev_volume
    FROM stock_prices
    WHERE date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
),

moving_averages AS (
    -- Calculate various moving averages
    SELECT 
        symbol,
        date,
        close_price,
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as sma_20,
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) as sma_50,
        STDDEV(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as std_dev_20
    FROM price_changes
),

bollinger_bands AS (
    -- Calculate Bollinger Bands
    SELECT 
        symbol,
        date,
        close_price,
        sma_20,
        sma_20 + (2 * std_dev_20) as upper_band,
        sma_20 - (2 * std_dev_20) as lower_band
    FROM moving_averages
),

volume_analysis AS (
    -- Analyze volume patterns
    SELECT 
        symbol,
        date,
        volume,
        AVG(volume) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as volume_sma,
        CASE 
            WHEN volume > 2 * AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) THEN 'High Volume'
            WHEN volume < 0.5 * AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) THEN 'Low Volume'
            ELSE 'Normal Volume'
        END as volume_pattern
    FROM price_changes
),

support_resistance AS (
    -- Identify support and resistance levels using recursive CTE
    WITH RECURSIVE price_levels AS (
        SELECT 
            symbol,
            date,
            close_price,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) as row_num,
            LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) as prev_price,
            LEAD(close_price) OVER (PARTITION BY symbol ORDER BY date) as next_price
        FROM price_changes
    ),
    levels AS (
        SELECT 
            symbol,
            date,
            close_price,
            row_num,
            prev_price,
            next_price,
            CASE 
                WHEN close_price > prev_price AND close_price > next_price THEN 'Resistance'
                WHEN close_price < prev_price AND close_price < next_price THEN 'Support'
                ELSE NULL
            END as level_type
        FROM price_levels
        WHERE row_num > 1
    )
    SELECT 
        symbol,
        date,
        close_price,
        level_type,
        COUNT(*) OVER (PARTITION BY symbol, level_type) as level_strength
    FROM levels
    WHERE level_type IS NOT NULL
)

-- Final comprehensive technical analysis
SELECT 
    bb.symbol,
    bb.date,
    bb.close_price,
    -- Moving averages
    bb.sma_20,
    bb.sma_50,
    -- Bollinger Bands
    bb.upper_band,
    bb.lower_band,
    -- Volume analysis
    va.volume_pattern,
    va.volume as current_volume,
    va.volume_sma as avg_volume,
    -- Support/Resistance
    sr.level_type as price_level,
    sr.level_strength,
    -- Calculate technical signals
    CASE 
        WHEN bb.close_price > bb.upper_band THEN 'Overbought'
        WHEN bb.close_price < bb.lower_band THEN 'Oversold'
        ELSE 'Neutral'
    END as bollinger_signal,
    CASE 
        WHEN bb.sma_20 > bb.sma_50 THEN 'Bullish'
        WHEN bb.sma_20 < bb.sma_50 THEN 'Bearish'
        ELSE 'Neutral'
    END as trend_signal,
    -- Calculate volatility
    (bb.upper_band - bb.lower_band) / bb.sma_20 as volatility_ratio,
    -- Calculate RSI
    calculate_rsi(bb.symbol, 14, DATE_SUB(bb.date, INTERVAL 30 DAY), bb.date) as rsi
FROM bollinger_bands bb
JOIN volume_analysis va USING (symbol, date)
LEFT JOIN support_resistance sr USING (symbol, date)
WHERE bb.date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
ORDER BY bb.symbol, bb.date;

-- Performance Notes:
-- 1. This query uses multiple CTEs for better organization and readability
-- 2. Window functions are used for moving averages and calculations
-- 3. Recursive CTE for support/resistance level identification
-- 4. Custom function for RSI calculation
-- 5. Consider partitioning tables by date for better performance
-- 6. Indexes on symbol and date will improve JOIN performance
-- 7. Consider materializing frequently accessed calculations 
