

-- 1. On charge tous nos modèles de "Staging" (via la macro ref de dbt)
WITH orders AS (
    SELECT * FROM "ecommerce_dw"."core"."stg_orders"
),

order_items AS (
    SELECT * FROM "ecommerce_dw"."core"."stg_order_items"
),

exchange_rates AS (
    SELECT * FROM "ecommerce_dw"."core"."stg_exchange_rates"
),

-- 2. La Transformation Métier : Le Cœur du Data Engineering
-- On regroupe les produits achetés par commande pour avoir le montant total (CA ou GMV)
order_totals AS (
    SELECT 
        order_id,
        COUNT(order_item_id) AS total_items,
        SUM(price_brl) AS total_price_brl,
        SUM(freight_brl) AS total_freight_brl
    FROM order_items
    GROUP BY 1
),

-- 3. La Table Finale : Jointures (Enrichissement)
final_fact AS (
    SELECT 
        o.order_id,
        o.customer_id,
        
        -- Dimensions temporelles
        o.purchase_ts AS order_date,
        o.status AS order_status,
        
        -- Métriques financières (Les Faits) venant de la sous-requête order_totals
        COALESCE(t.total_items, 0) AS total_items,
        COALESCE(t.total_price_brl, 0) AS revenue_brl,
        
        -- ⭐ LA MAGIE : L'enrichissement via API !
        -- On convertit le Chiffre d'Affaires avec le taux exact du jour de la commande
        ROUND(COALESCE(t.total_price_brl, 0) * r.usd_rate, 2) AS revenue_usd,
        
        r.usd_rate AS current_exchange_rate
        
    FROM orders o
    -- Jointure avec les totaux par commande (Lignes de facture)
    LEFT JOIN order_totals t 
        ON o.order_id = t.order_id
        
    -- Jointure avec l'API (On compare uniquement l'année-mois-jour, pas l'heure)
    LEFT JOIN exchange_rates r 
        ON DATE_TRUNC('day', o.purchase_ts) = r.rate_date
)

SELECT * FROM final_fact