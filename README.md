# Financial Market Analysis

This project demonstrates advanced MySQL techniques for analyzing financial market data, including stock prices, trading volumes, and market indicators. The analysis focuses on technical analysis, market trends, and trading patterns.

## Project Overview

This project analyzes financial market data to provide insights into:
- Stock price movements and trends
- Trading volume analysis
- Technical indicators calculation
- Market sentiment analysis
- Trading pattern recognition

## Key SQL Concepts Demonstrated

1. **Complex Joins and Window Functions**
   - Self-joins for price comparisons
   - Window functions for moving averages
   - Recursive CTEs for pattern recognition

2. **Performance Optimization**
   - Custom indexes
   - Query optimization techniques
   - Materialized views
   - Partitioning strategies

3. **Custom Functions**
   - Technical indicator calculations
   - Pattern recognition algorithms
   - Statistical functions

4. **Data Cleaning and Transformation**
   - Handling missing values
   - Data normalization
   - Time series manipulation

5. **Advanced Analytics**
   - Moving averages
   - Volatility calculations
   - Trend analysis
   - Pattern matching

## Dataset Schema

```sql
-- Stock prices table
CREATE TABLE stock_prices (
    symbol VARCHAR(10),
    date DATE,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT,
    adjusted_close DECIMAL(10,2),
    INDEX idx_symbol_date (symbol, date)
) PARTITION BY RANGE (YEAR(date));

-- Technical indicators table
CREATE TABLE technical_indicators (
    symbol VARCHAR(10),
    date DATE,
    sma_20 DECIMAL(10,2),
    sma_50 DECIMAL(10,2),
    rsi DECIMAL(5,2),
    macd DECIMAL(10,2),
    macd_signal DECIMAL(10,2),
    macd_hist DECIMAL(10,2),
    bollinger_upper DECIMAL(10,2),
    bollinger_lower DECIMAL(10,2),
    INDEX idx_symbol_date (symbol, date)
);

-- Market sentiment table
CREATE TABLE market_sentiment (
    symbol VARCHAR(10),
    date DATE,
    sentiment_score DECIMAL(5,2),
    news_count INT,
    social_volume INT,
    INDEX idx_symbol_date (symbol, date)
);

-- Trading patterns table
CREATE TABLE trading_patterns (
    pattern_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    pattern_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    confidence_score DECIMAL(5,2),
    INDEX idx_symbol_date (symbol, start_date)
);
```

## Analysis Queries

The project includes several SQL queries demonstrating different aspects of financial market analysis:

1. **Technical Analysis**
   - Moving average calculations
   - RSI and MACD indicators
   - Bollinger Bands
   - Volume analysis

2. **Pattern Recognition**
   - Support and resistance levels
   - Trend identification
   - Chart pattern detection
   - Breakout analysis

3. **Market Sentiment**
   - News sentiment analysis
   - Social media impact
   - Volume profile analysis
   - Market breadth indicators

4. **Performance Metrics**
   - Returns calculation
   - Risk metrics
   - Sharpe ratio
   - Drawdown analysis

## Getting Started

1. Set up your MySQL environment (version 8.0 or higher)
2. Create the necessary tables using the schema provided
3. Execute the queries in the `queries/` directory
4. Review the results and insights

## Performance Considerations

- Use appropriate partitioning on date columns
- Implement materialized views for frequently accessed metrics
- Optimize JOIN operations with proper indexing
- Use window functions efficiently
- Consider using stored procedures for complex calculations

## Results and Insights

The analysis provides insights into:
- Market trends and patterns
- Technical indicators and signals
- Trading opportunities
- Risk management metrics
- Market sentiment indicators

## Contributing

Feel free to submit issues and enhancement requests! 