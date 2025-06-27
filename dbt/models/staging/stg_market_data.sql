{{ config(materialized='view') }}

WITH latest_prices AS (
    SELECT 
        symbol,
        price,
        volume,
        open_price,
        high_price,
        low_price,
        close_price,
        adjusted_close,
        timestamp,
        source,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
    FROM {{ source('raw', 'raw_market_data') }}
    WHERE 
        price > {{ var('price_validity_min') }}
        AND price < {{ var('price_validity_max') }}
        AND timestamp >= CURRENT_DATE - INTERVAL '{{ var('lookback_years') }} years'
),

price_changes AS (
    SELECT 
        *,
        LAG(price) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
        CASE 
            WHEN LAG(price) OVER (PARTITION BY symbol ORDER BY timestamp) IS NOT NULL 
            THEN (price - LAG(price) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(price) OVER (PARTITION BY symbol ORDER BY timestamp)
            ELSE 0 
        END as price_change_pct
    FROM latest_prices
)

SELECT 
    symbol,
    price,
    prev_price,
    price_change_pct,
    volume,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close,
    timestamp,
    source,
    created_at,
    rn,
    -- Add data quality flags
    CASE 
        WHEN timestamp >= CURRENT_TIMESTAMP - INTERVAL '{{ var('freshness_threshold_hours') }} hours' 
        THEN true 
        ELSE false 
    END as is_fresh,
    
    CASE 
        WHEN volume > 0 
        THEN true 
        ELSE false 
    END as has_volume
    
FROM price_changes
