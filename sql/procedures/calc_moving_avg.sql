-- ── Stored procedure: calculate moving averages ───────────
-- Recalculates moving averages for a specific symbol.
-- Called by Airflow after new data lands in Silver.
-- In Spark SQL we use a macro pattern instead of PROCEDURE.

-- 7-day moving average for a given symbol
CREATE OR REPLACE TEMP VIEW mv_7d_{symbol} AS
SELECT
    symbol,
    date,
    close,
    ROUND(
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4
    ) AS ma_7d
FROM stock_summary
WHERE symbol = '{symbol}'
ORDER BY date;

-- 30-day moving average for a given symbol
CREATE OR REPLACE TEMP VIEW mv_30d_{symbol} AS
SELECT
    symbol,
    date,
    close,
    ROUND(
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4
    ) AS ma_30d
FROM stock_summary
WHERE symbol = '{symbol}'
ORDER BY date;

-- Combined moving averages with crossover signal
-- Golden cross: 7d MA crosses above 30d MA = bullish signal
-- Death cross:  7d MA crosses below 30d MA = bearish signal
CREATE OR REPLACE TEMP VIEW mv_crossover_{symbol} AS
SELECT
    a.symbol,
    a.date,
    a.close,
    a.ma_7d,
    b.ma_30d,
    CASE
        WHEN a.ma_7d > b.ma_30d THEN 'GOLDEN CROSS'
        WHEN a.ma_7d < b.ma_30d THEN 'DEATH CROSS'
        ELSE                         'NEUTRAL'
    END AS crossover_signal
FROM mv_7d_{symbol}  a
JOIN mv_30d_{symbol} b
  ON a.symbol = b.symbol
 AND a.date   = b.date
ORDER BY a.date;