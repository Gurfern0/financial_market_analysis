-- Advanced Market Sentiment Analysis and Trading Pattern Recognition
-- This query demonstrates complex aggregations, pattern matching, and sentiment analysis

-- Create a function for calculating sentiment momentum
DELIMITER //
CREATE FUNCTION calculate_sentiment_momentum(
    symbol VARCHAR(10),
    period INT,
    start_date DATE,
    end_date DATE
) RETURNS DECIMAL(5,2)
DETERMINISTIC
BEGIN
    DECLARE current_sentiment DECIMAL(5,2);
    DECLARE previous_sentiment DECIMAL(5,2);
    
    -- Get current and previous sentiment
    SELECT 
        sentiment_score,
        LAG(sentiment_score) OVER (ORDER BY date)
    INTO current_sentiment, previous_sentiment
    FROM market_sentiment
    WHERE symbol = symbol
    AND date BETWEEN start_date AND end_date
    ORDER BY date DESC
    LIMIT 1;
    
    -- Calculate momentum
    RETURN (current_sentiment - previous_sentiment) / period;
END //
DELIMITER ;

-- Main analysis query
WITH sentiment_analysis AS (
    -- Analyze sentiment trends and patterns
    SELECT 
        ms.symbol,
        ms.date,
        ms.sentiment_score,
        ms.news_count,
        ms.social_volume,
        -- Calculate sentiment moving averages
        AVG(ms.sentiment_score) OVER (
            PARTITION BY ms.symbol 
            ORDER BY ms.date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as sentiment_sma_5,
        AVG(ms.sentiment_score) OVER (
            PARTITION BY ms.symbol 
            ORDER BY ms.date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as sentiment_sma_10,
        -- Calculate sentiment volatility
        STDDEV(ms.sentiment_score) OVER (
            PARTITION BY ms.symbol 
            ORDER BY ms.date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as sentiment_volatility,
        -- Calculate news and social media momentum
        (ms.news_count - LAG(ms.news_count) OVER (
            PARTITION BY ms.symbol 
            ORDER BY ms.date
        )) as news_momentum,
        (ms.social_volume - LAG(ms.social_volume) OVER (
            PARTITION BY ms.symbol 
            ORDER BY ms.date
        )) as social_momentum
    FROM market_sentiment ms
    WHERE ms.date >= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
),

price_patterns AS (
    -- Identify common chart patterns using recursive CTE
    WITH RECURSIVE pattern_points AS (
        SELECT 
            sp.symbol,
            sp.date,
            sp.close_price,
            ROW_NUMBER() OVER (PARTITION BY sp.symbol ORDER BY sp.date) as point_num,
            LAG(sp.close_price) OVER (PARTITION BY sp.symbol ORDER BY sp.date) as prev_price,
            LEAD(sp.close_price) OVER (PARTITION BY sp.symbol ORDER BY sp.date) as next_price
        FROM stock_prices sp
        WHERE sp.date >= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
    ),
    pattern_detection AS (
        SELECT 
            symbol,
            date,
            close_price,
            point_num,
            prev_price,
            next_price,
            -- Detect double top pattern
            CASE 
                WHEN close_price > prev_price AND close_price > next_price 
                AND LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) > 
                    LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                AND LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) > 
                    LAG(close_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                THEN 'Double Top'
                -- Detect double bottom pattern
                WHEN close_price < prev_price AND close_price < next_price 
                AND LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) < 
                    LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date)
                AND LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) < 
                    LAG(close_price, 3) OVER (PARTITION BY symbol ORDER BY date)
                THEN 'Double Bottom'
                ELSE NULL
            END as pattern_type
        FROM pattern_points
        WHERE point_num > 3
    )
    SELECT 
        symbol,
        date,
        close_price,
        pattern_type,
        -- Calculate pattern strength
        CASE 
            WHEN pattern_type = 'Double Top' THEN 
                (close_price - LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date)) / 
                LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date)
            WHEN pattern_type = 'Double Bottom' THEN 
                (LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date) - close_price) / 
                LAG(close_price, 2) OVER (PARTITION BY symbol ORDER BY date)
            ELSE 0
        END as pattern_strength
    FROM pattern_detection
    WHERE pattern_type IS NOT NULL
),

volume_profile AS (
    -- Analyze volume profile and distribution
    SELECT 
        sp.symbol,
        sp.date,
        sp.close_price,
        sp.volume,
        -- Calculate volume profile
        SUM(sp.volume) OVER (
            PARTITION BY sp.symbol 
            ORDER BY sp.date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as volume_profile_5d,
        -- Calculate volume distribution
        PERCENT_RANK() OVER (
            PARTITION BY sp.symbol 
            ORDER BY sp.volume
        ) as volume_percentile,
        -- Calculate volume trend
        CASE 
            WHEN sp.volume > AVG(sp.volume) OVER (
                PARTITION BY sp.symbol 
                ORDER BY sp.date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) THEN 'Increasing'
            WHEN sp.volume < AVG(sp.volume) OVER (
                PARTITION BY sp.symbol 
                ORDER BY sp.date 
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) THEN 'Decreasing'
            ELSE 'Stable'
        END as volume_trend
    FROM stock_prices sp
    WHERE sp.date >= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
)

-- Final comprehensive market sentiment analysis
SELECT 
    sa.symbol,
    sa.date,
    -- Sentiment metrics
    sa.sentiment_score,
    sa.sentiment_sma_5,
    sa.sentiment_sma_10,
    sa.sentiment_volatility,
    sa.news_count,
    sa.social_volume,
    -- Pattern analysis
    pp.pattern_type,
    pp.pattern_strength,
    -- Volume analysis
    vp.volume_profile_5d,
    vp.volume_percentile,
    vp.volume_trend,
    -- Calculate sentiment momentum
    calculate_sentiment_momentum(sa.symbol, 5, 
        DATE_SUB(sa.date, INTERVAL 5 DAY), sa.date) as sentiment_momentum,
    -- Calculate market sentiment score
    (
        (sa.sentiment_score * 0.3) +
        (sa.sentiment_momentum * 0.2) +
        (CASE 
            WHEN pp.pattern_type = 'Double Top' THEN -0.2
            WHEN pp.pattern_type = 'Double Bottom' THEN 0.2
            ELSE 0
        END) +
        (CASE 
            WHEN vp.volume_trend = 'Increasing' THEN 0.15
            WHEN vp.volume_trend = 'Decreasing' THEN -0.15
            ELSE 0
        END) +
        (CASE 
            WHEN sa.news_momentum > 0 THEN 0.15
            WHEN sa.news_momentum < 0 THEN -0.15
            ELSE 0
        END)
    ) as market_sentiment_score
FROM sentiment_analysis sa
LEFT JOIN price_patterns pp USING (symbol, date)
LEFT JOIN volume_profile vp USING (symbol, date)
WHERE sa.date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
ORDER BY sa.symbol, sa.date;

-- Performance Notes:
-- 1. This query uses multiple CTEs for complex calculations
-- 2. Custom function for sentiment momentum calculation
-- 3. Recursive CTE for pattern detection
-- 4. Window functions for moving averages and trends
-- 5. Consider partitioning tables by date
-- 6. Indexes on symbol and date will improve performance
-- 7. Consider materializing frequently accessed calculations
-- 8. Optimize JOIN operations with proper indexing 