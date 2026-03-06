WITH cte_1 AS (
    -- CTE-1: user_id, game_name, payment_month, revenue за місяць
    SELECT
        gp.user_id,
        gp.game_name,
        date_trunc('month', gp.payment_date)::date AS payment_month,
        SUM(gp.revenue_amount_usd) AS revenue_usd
    FROM project.games_payments gp
    GROUP BY 1, 2, 3
),

cte_2 AS (
    -- CTE-2: додаємо попередній/наступний місяць оплати, календарні місяці, prev revenue, first month
    SELECT
        c1.*,

        LAG(c1.payment_month)  OVER (PARTITION BY c1.user_id, c1.game_name ORDER BY c1.payment_month) AS prev_payment_month,
        LEAD(c1.payment_month) OVER (PARTITION BY c1.user_id, c1.game_name ORDER BY c1.payment_month) AS next_payment_month,

        (c1.payment_month - INTERVAL '1 month')::date AS prev_calendar_month,
        (c1.payment_month + INTERVAL '1 month')::date AS next_calendar_month,

        LAG(c1.revenue_usd) OVER (PARTITION BY c1.user_id, c1.game_name ORDER BY c1.payment_month) AS prev_revenue_usd,

        MIN(c1.payment_month) OVER (PARTITION BY c1.user_id, c1.game_name) AS first_payment_month
    FROM cte_1 c1
),

cte_3 AS (
    -- CTE-3: case when метрики на рівні user-month
    SELECT
        c2.*,

        1 AS is_paid_user,

        CASE WHEN c2.payment_month = c2.first_payment_month THEN 1 ELSE 0 END AS is_new_paid_user,
        CASE WHEN c2.payment_month = c2.first_payment_month THEN c2.revenue_usd ELSE 0 END AS new_mrr_usd,

        -- churn: якщо немає оплати в наступному календарному місяці
        CASE
            WHEN c2.next_payment_month IS NULL THEN 1
            WHEN c2.next_payment_month <> c2.next_calendar_month THEN 1
            ELSE 0
        END AS is_churned_user,

        -- churned revenue (потім зсуваємо в Tableau на +1 місяць)
        CASE
            WHEN (c2.next_payment_month IS NULL OR c2.next_payment_month <> c2.next_calendar_month)
                THEN c2.revenue_usd
            ELSE 0
        END AS churned_revenue_usd,

        -- expansion / contraction: тільки якщо минулий місяць = попередній календарний (без пропуску)
        CASE
            WHEN c2.prev_payment_month = c2.prev_calendar_month
                 AND c2.prev_revenue_usd IS NOT NULL
                 AND c2.revenue_usd > c2.prev_revenue_usd
                THEN (c2.revenue_usd - c2.prev_revenue_usd)
            ELSE 0
        END AS expansion_mrr_usd,

        CASE
            WHEN c2.prev_payment_month = c2.prev_calendar_month
                 AND c2.prev_revenue_usd IS NOT NULL
                 AND c2.revenue_usd < c2.prev_revenue_usd
                THEN (c2.prev_revenue_usd - c2.revenue_usd)
            ELSE 0
        END AS contraction_mrr_usd

    FROM cte_2 c2
)

SELECT
    c3.user_id,
    c3.game_name,
    c3.payment_month,
    c3.revenue_usd,

    -- метрики (user-month)
    c3.is_paid_user,
    c3.is_new_paid_user,
    c3.new_mrr_usd,
    c3.is_churned_user,
    c3.churned_revenue_usd,
    c3.expansion_mrr_usd,
    c3.contraction_mrr_usd,
    c3.prev_payment_month,
    c3.next_payment_month,
    c3.first_payment_month,
    
    -- поля для фільтрів
    gpu.language,
    gpu.age,
    gpu.has_older_device_model


FROM cte_3 c3
LEFT JOIN project.games_paid_users gpu
    ON gpu.user_id = c3.user_id
   AND gpu.game_name = c3.game_name
;