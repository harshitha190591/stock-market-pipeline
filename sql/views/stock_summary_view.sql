-- ── Stock summary view ────────────────────────────────────
-- Provides a clean analytical view on top of the Gold table.
-- Analysts query this view instead of the raw Delta table.

CREATE OR REPLACE VIEW vw_stock_summary AS
SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,

    -- daily return as a readable percentage
    ROUND(daily_return_pct, 2)          AS daily_return_pct,

    -- intraday volatility
    ROUND(price_range, 2)               AS price_range,

    -- moving averages
    ROUND(ma_7d,  2)                    AS ma_7d,
    ROUND(ma_30d, 2)                    AS ma_30d,
    ROUND(ma_90d, 2)                    AS ma_90d,

    -- signal: is close above 30-day moving average?
    -- above = bullish trend, below = bearish trend
    CASE
        WHEN close > ma_30d THEN 'ABOVE'
        WHEN close < ma_30d THEN 'BELOW'
        ELSE                     'AT'
    END                                 AS ma30_signal,

    -- classify daily performance
    CASE
        WHEN daily_return_pct >= 3   THEN 'STRONG GAIN'
        WHEN daily_return_pct >= 1   THEN 'GAIN'
        WHEN daily_return_pct <= -3  THEN 'STRONG LOSS'
        WHEN daily_return_pct <= -1  THEN 'LOSS'
        ELSE                              'FLAT'
    END                                 AS performance_label,

    price_spike_flag,
    _processed_at

FROM stock_summary;


-- ── Top movers view ───────────────────────────────────────
-- Shows best and worst performing stocks per day.

CREATE OR REPLACE VIEW vw_top_movers AS
SELECT
    date,
    symbol,
    direction,
    ROUND(daily_return_pct, 2)          AS daily_return_pct,
    ROUND(close, 2)                     AS close,
    volume,
    rank
FROM top_movers
WHERE rank <= 5                         -- top 5 gainers and losers
ORDER BY date DESC, direction, rank;


-- ── Volume spikes view ────────────────────────────────────
-- Shows days with unusually high trading volume.

CREATE OR REPLACE VIEW vw_volume_spikes AS
SELECT
    symbol,
    date,
    volume,
    ROUND(avg_30d_volume, 0)            AS avg_30d_volume,
    ROUND(volume / avg_30d_volume, 2)   AS volume_ratio,
    ROUND(daily_return_pct, 2)          AS daily_return_pct,
    close
FROM volume_analysis
ORDER BY volume_ratio DESC;