-- models/intermediate/int_user_metrics.sql
-- ─────────────────────────────────────────
-- Agrega métricas por usuário a partir dos eventos limpos.
-- Base para o cálculo de RFM e segmentação.

{{ config(materialized='table') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_behavioral_events') }}
),

-- Compras (base para monetary e frequency RFM)
purchases AS (
    SELECT
        user_id,
        COUNT(DISTINCT DATE(event_time))    AS purchase_days,
        COUNT(*)                            AS total_orders,
        SUM(price)                          AS total_revenue,
        AVG(price)                          AS avg_order_value,
        MAX(event_time)                     AS last_purchase_at,
        MIN(event_time)                     AS first_purchase_at
    FROM events
    WHERE event_type = 'purchase' AND price IS NOT NULL
    GROUP BY user_id
),

-- Views e navegação
navigation AS (
    SELECT
        user_id,
        COUNT(*)                            AS total_views,
        COUNT(DISTINCT product_id)          AS unique_products_viewed,
        COUNT(DISTINCT category_code)       AS unique_categories_viewed,
        MIN(event_time)                     AS first_seen,
        MAX(event_time)                     AS last_seen
    FROM events
    WHERE event_type = 'view'
    GROUP BY user_id
),

-- Carrinho
cart AS (
    SELECT
        user_id,
        COUNT(*)    AS total_carts
    FROM events
    WHERE event_type = 'cart'
    GROUP BY user_id
),

-- Categoria favorita (mais comprada)
fav_category AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        category_code   AS favorite_category,
        COUNT(*)        AS category_purchases
    FROM events
    WHERE event_type = 'purchase' AND category_code IS NOT NULL
    GROUP BY user_id, category_code
    ORDER BY user_id, COUNT(*) DESC
),

-- Marca favorita
fav_brand AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        brand           AS favorite_brand,
        COUNT(*)        AS brand_purchases
    FROM events
    WHERE event_type = 'purchase' AND brand IS NOT NULL
    GROUP BY user_id, brand
    ORDER BY user_id, COUNT(*) DESC
),

-- Sessões únicas
sessions AS (
    SELECT
        user_id,
        COUNT(DISTINCT user_session)    AS unique_sessions,
        COUNT(DISTINCT DATE(event_time)) AS days_active
    FROM events
    GROUP BY user_id
),

-- Junta tudo
combined AS (
    SELECT
        COALESCE(nav.user_id, p.user_id, c.user_id)    AS user_id,

        -- Engajamento
        COALESCE(nav.total_views, 0)                    AS total_views,
        COALESCE(nav.unique_products_viewed, 0)         AS unique_products_viewed,
        COALESCE(nav.unique_categories_viewed, 0)       AS unique_categories_viewed,
        COALESCE(c.total_carts, 0)                      AS total_carts,
        COALESCE(ses.unique_sessions, 0)                AS unique_sessions,
        COALESCE(ses.days_active, 0)                    AS days_active,

        -- Compras
        COALESCE(p.total_orders, 0)                     AS total_purchases,
        COALESCE(p.total_revenue, 0)                    AS total_revenue,
        p.avg_order_value,
        p.last_purchase_at,
        p.first_purchase_at,

        -- Atividade geral
        COALESCE(nav.first_seen, p.first_purchase_at)   AS first_seen,
        COALESCE(nav.last_seen, p.last_purchase_at)     AS last_seen,

        -- Preferências
        fc.favorite_category,
        fb.favorite_brand,

        -- Taxas
        CASE
            WHEN COALESCE(c.total_carts, 0) > 0
            THEN COALESCE(p.total_orders, 0)::NUMERIC / c.total_carts
            ELSE 0
        END AS cart_conversion_rate

    FROM navigation   nav
    FULL OUTER JOIN purchases p   ON nav.user_id = p.user_id
    FULL OUTER JOIN cart      c   ON COALESCE(nav.user_id, p.user_id) = c.user_id
    LEFT JOIN sessions        ses ON COALESCE(nav.user_id, p.user_id) = ses.user_id
    LEFT JOIN fav_category    fc  ON COALESCE(nav.user_id, p.user_id) = fc.user_id
    LEFT JOIN fav_brand       fb  ON COALESCE(nav.user_id, p.user_id) = fb.user_id
)

SELECT
    user_id,
    total_views,
    unique_products_viewed,
    unique_categories_viewed,
    total_carts,
    total_purchases,
    total_revenue,
    avg_order_value,
    unique_sessions,
    days_active,
    first_seen,
    last_seen,
    last_purchase_at,
    first_purchase_at,
    favorite_category,
    favorite_brand,
    ROUND(cart_conversion_rate, 4) AS cart_conversion_rate,
    NOW()                          AS updated_at
FROM combined
WHERE user_id IS NOT NULL