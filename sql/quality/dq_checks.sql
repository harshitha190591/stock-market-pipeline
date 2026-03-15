-- ── SQL Data Quality Checks ───────────────────────────────
-- These checks run after Silver is written.
-- Each check returns a count — zero means PASSED.
-- Non-zero means FAILED — Airflow blocks Gold from running.


-- Check 1: No null symbols
-- Every row must have a stock symbol
SELECT
    'null_symbols'      AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM stock_prices_silver
WHERE symbol IS NULL;


-- Check 2: No null dates
SELECT
    'null_dates'        AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM stock_prices_silver
WHERE date IS NULL;


-- Check 3: No negative prices
-- Open, high, low, close must all be positive
SELECT
    'negative_prices'   AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM stock_prices_silver
WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0;


-- Check 4: High must be >= Low
-- A stock's daily high cannot be lower than its daily low
SELECT
    'high_lt_low'       AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM stock_prices_silver
WHERE high < low;


-- Check 5: No duplicate symbol + date combinations
SELECT
    'duplicates'        AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM (
    SELECT symbol, date, COUNT(*) AS cnt
    FROM stock_prices_silver
    GROUP BY symbol, date
    HAVING cnt > 1
) dupes;


-- Check 6: All expected symbols are present
SELECT
    'missing_symbols'   AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM (
    SELECT explode(array('AAPL','TSLA','gogl','MSFT','AMZN')) AS expected_symbol
) expected
WHERE expected_symbol NOT IN (
    SELECT DISTINCT symbol FROM stock_prices_silver
);


-- Check 7: Row count per symbol must be above minimum
SELECT
    'low_row_count'     AS check_name,
    COUNT(*)            AS fail_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS status
FROM (
    SELECT symbol, COUNT(*) AS cnt
    FROM stock_prices_silver
    GROUP BY symbol
    HAVING cnt < 100
) low_counts;