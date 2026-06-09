-- models/marts/mart_rfm_base.sql
-- ─────────────────────────────────────────
-- Calcula os valores brutos de Recency, Frequency e Monetary
-- para cada usuário. Os scores (1-5) e segmentos são calculados
-- pelo Python (rfm_compute.py) para maior flexibilidade.

{{ config(materialized='table') }}

WITH metrics AS (
    SELECT * FROM {{ ref('int_user_metrics') }}
),

rfm_raw AS (
    SELECT
        user_id,

        -- Recency: dias desde última compra
        CASE
            WHEN last_purchase_at IS NULL THEN 999
            ELSE EXTRACT(DAY FROM (NOW() - last_purchase_at))::INT
        END AS recency_days,

        -- Frequency: total de compras
        total_purchases AS frequency,

        -- Monetary: total gasto
        COALESCE(total_revenue, 0) AS monetary,

        -- Contexto adicional para ML
        total_views,
        total_carts,
        unique_sessions,
        days_active,
        cart_conversion_rate,
        avg_order_value,
        favorite_category,
        favorite_brand,
        first_seen,
        last_seen,
        last_purchase_at
    FROM metrics
)

SELECT
    user_id,
    recency_days,
    frequency,
    ROUND(monetary, 2)          AS monetary,
    total_views,
    total_carts,
    unique_sessions,
    days_active,
    ROUND(cart_conversion_rate, 4) AS cart_conversion_rate,
    ROUND(avg_order_value, 2)   AS avg_order_value,
    favorite_category,
    favorite_brand,
    first_seen,
    last_seen,
    last_purchase_at,
    NOW()                       AS computed_at
FROM rfm_raw
ORDER BY monetary DESC