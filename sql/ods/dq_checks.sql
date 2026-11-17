-- Проверки качества слоя ods за дату ds (ФТ-10). Параметр: %(ds)s
-- Каждая строка: имя проверки и число нарушений. Задача DAG падает, если хоть одно > 0.

SELECT 'ods_trades: trade_id unique' AS check_name, count(*) AS violations
FROM (SELECT trade_id FROM orozova_ods.ods_trades WHERE _ds = %(ds)s GROUP BY trade_id HAVING count(*) > 1) d
UNION ALL
SELECT 'ods_trades: keys not null', count(*)
FROM orozova_ods.ods_trades
WHERE _ds = %(ds)s AND (trade_id IS NULL OR user_id IS NULL OR coin_id IS NULL OR trade_ts IS NULL)
UNION ALL
SELECT 'ods_trades: status in (completed, failed, cancelled)', count(*)
FROM orozova_ods.ods_trades
WHERE _ds = %(ds)s AND status NOT IN ('completed', 'failed', 'cancelled')
UNION ALL
SELECT 'ods_trades: side in (buy, sell)', count(*)
FROM orozova_ods.ods_trades
WHERE _ds = %(ds)s AND side NOT IN ('buy', 'sell')
UNION ALL
SELECT 'ods_trades: amount_eur > 0', count(*)
FROM orozova_ods.ods_trades
WHERE _ds = %(ds)s AND amount_eur <= 0
UNION ALL
SELECT 'ods_trades: user exists in ods_users', count(*)
FROM orozova_ods.ods_trades t
LEFT JOIN orozova_ods.ods_users u USING (user_id)
WHERE t._ds = %(ds)s AND u.user_id IS NULL
UNION ALL
SELECT 'ods_users: plan in allowed values', count(*)
FROM orozova_ods.ods_users
WHERE plan NOT IN ('standard', 'plus', 'premium', 'metal', 'ultra')
UNION ALL
SELECT 'ods_coin_prices: 6 coins for ds', abs(6 - count(*))
FROM orozova_ods.ods_coin_prices
WHERE price_date = %(ds)s;
