-- ods → dm_monthly_country_plan: пересчитывается весь месяц, в который входит ds
-- (идемпотентно: удалить месяц → вставить). Параметр: %(ds)s

DELETE FROM orozova_datamart.dm_monthly_country_plan
WHERE month = date_trunc('month', %(ds)s::date)::date;

INSERT INTO orozova_datamart.dm_monthly_country_plan
    (month, country_code, plan, active_traders, verified_users, conversion_to_trade,
     completed_cnt, volume_eur, fee_revenue_eur, take_rate, avg_btc_price_eur, _ds, _built_at)
WITH bounds AS (
    SELECT date_trunc('month', %(ds)s::date)::date AS m_start,
           (date_trunc('month', %(ds)s::date) + interval '1 month')::date AS m_end
),
trades AS (
    SELECT u.country_code, u.plan, t.user_id, t.amount_eur, t.fee_eur
    FROM orozova_ods.ods_trades t
    JOIN orozova_ods.ods_users u USING (user_id)
    CROSS JOIN bounds b
    WHERE t.trade_date >= b.m_start AND t.trade_date < b.m_end
      AND t.status = 'completed'
),
agg AS (
    SELECT country_code, plan,
           count(DISTINCT user_id) AS active_traders,
           count(*)                AS completed_cnt,
           sum(amount_eur)         AS volume_eur,
           sum(fee_eur)            AS fee_revenue_eur
    FROM trades
    GROUP BY country_code, plan
),
verified AS (
    SELECT u.country_code, u.plan, count(*) AS verified_users
    FROM orozova_ods.ods_users u
    JOIN orozova_ods.ods_users_kyc k USING (user_id)
    WHERE k.kyc_status = 'verified'
    GROUP BY u.country_code, u.plan
),
btc AS (
    SELECT avg(price_eur) AS avg_price
    FROM orozova_ods.ods_coin_prices, bounds b
    WHERE coin_id = 'bitcoin' AND price_date >= b.m_start AND price_date < b.m_end
)
SELECT
    (SELECT m_start FROM bounds),
    a.country_code,
    a.plan,
    a.active_traders,
    coalesce(v.verified_users, 0),
    round(a.active_traders::numeric / nullif(v.verified_users, 0), 4),
    a.completed_cnt,
    a.volume_eur,
    a.fee_revenue_eur,
    round(a.fee_revenue_eur / nullif(a.volume_eur, 0), 6),
    (SELECT avg_price FROM btc),
    %(ds)s::date,
    now()
FROM agg a
LEFT JOIN verified v USING (country_code, plan);
