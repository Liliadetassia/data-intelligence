-- models/marts/mart_product_metrics.sql
-- ─────────────────────────────────────────
-- Métricas de performance por produto.
-- Usado pelo motor de recomendação e dashboard.

{{ config(materialized='table') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_behavioral_events') }}
),

product_stats AS (
    SELECT
        product_id,
        category_code,
        brand,

        -- Preço médio praticado
        AVG(CASE WHEN event_type = 'purchase' THEN price END)   AS avg_price,
        MIN(CASE WHEN event_type = 'purchase' THEN price END)   AS min_price,
        MAX(CASE WHEN event_type = 'purchase' THEN price END)   AS max_price,

        -- Funil
        COUNT(CASE WHEN event_type = 'view'     THEN 1 END)     AS total_views,
        COUNT(CASE WHEN event_type = 'cart'     THEN 1 END)     AS total_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)     AS total_purchases,

        -- Receita
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue_total,

        -- Usuários únicos
        COUNT(DISTINCT user_id)                                 AS unique_users,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'
              THEN user_id END)                                  AS unique_buyers

    FROM events
    WHERE product_id IS NOT NULL
    GROUP BY product_id, category_code, brand
)

SELECT
    product_id,
    category_code,
    brand,
    ROUND(avg_price, 2)     AS avg_price,
    ROUND(min_price, 2)     AS min_price,
    ROUND(max_price, 2)     AS max_price,
    total_views,
    total_carts,
    total_purchases,
    unique_users,
    unique_buyers,
    ROUND(revenue_total, 2) AS revenue_total,

    -- Taxas de conversão
    CASE WHEN total_views > 0
         THEN ROUND(total_purchases::NUMERIC / total_views, 4)
         ELSE 0
    END AS view_to_purchase_rate,

    CASE WHEN total_carts > 0
         THEN ROUND(total_purchases::NUMERIC / total_carts, 4)
         ELSE 0
    END AS cart_to_purchase_rate,

    NOW() AS computed_at
FROM product_stats
ORDER BY revenue_total DESC