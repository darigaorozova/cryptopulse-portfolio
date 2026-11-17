-- ods → dm_daily_crypto_activity за дату ds (идемпотентно). Параметр: %(ds)s

DELETE FROM orozova_datamart.dm_daily_crypto_activity WHERE trade_date = %(ds)s;

INSERT INTO orozova_datamart.dm_daily_crypto_activity
    (trade_date, active_traders, trades_cnt, completed_cnt, buy_volume_eur, sell_volume_eur,
     volume_eur, net_flow_eur, fee_revenue_eur, failed_share, btc_price_eur, btc_change_pct, _ds, _built_at)
WITH t AS (
    SELECT *
    FROM orozova_ods.ods_trades
    WHERE trade_date = %(ds)s
),
btc AS (
    SELECT
        max(price_eur) FILTER (WHERE price_date = %(ds)s::date)                      AS price_today,
        max(price_eur) FILTER (WHERE price_date = %(ds)s::date - 1)                  AS price_prev
    FROM orozova_ods.ods_coin_prices
    WHERE coin_id = 'bitcoin' AND price_date IN (%(ds)s::date, %(ds)s::date - 1)
)
SELECT
    %(ds)s::date,
    count(DISTINCT user_id) FILTER (WHERE status = 'completed'),
    count(*),
    count(*) FILTER (WHERE status = 'completed'),
    coalesce(sum(amount_eur) FILTER (WHERE status = 'completed' AND side = 'buy'), 0),
    coalesce(sum(amount_eur) FILTER (WHERE status = 'completed' AND side = 'sell'), 0),
    coalesce(sum(amount_eur) FILTER (WHERE status = 'completed'), 0),
    coalesce(sum(CASE WHEN side = 'buy' THEN amount_eur ELSE -amount_eur END)
             FILTER (WHERE status = 'completed'), 0),
    coalesce(sum(fee_eur) FILTER (WHERE status = 'completed'), 0),
    coalesce(round(count(*) FILTER (WHERE status = 'failed')::numeric / nullif(count(*), 0), 4), 0),
    (SELECT price_today FROM btc),
    (SELECT round((price_today / nullif(price_prev, 0) - 1) * 100, 4) FROM btc),
    %(ds)s::date,
    now()
FROM t
HAVING count(*) > 0;
