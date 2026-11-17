-- Проверки качества витрин (ФТ-10) за дату ds. Параметр: %(ds)s

SELECT 'dm_daily: trade_date unique' AS check_name, count(*) AS violations
FROM (SELECT trade_date FROM orozova_datamart.dm_daily_crypto_activity GROUP BY trade_date HAVING count(*) > 1) d
UNION ALL
SELECT 'dm_daily: row for ds exists', CASE WHEN EXISTS (
    SELECT 1 FROM orozova_datamart.dm_daily_crypto_activity WHERE trade_date = %(ds)s) THEN 0 ELSE 1 END
UNION ALL
SELECT 'dm_monthly: (month, country, plan) unique', count(*)
FROM (SELECT month, country_code, plan FROM orozova_datamart.dm_monthly_country_plan
      GROUP BY month, country_code, plan HAVING count(*) > 1) d
UNION ALL
SELECT 'dm_monthly: plan in allowed values', count(*)
FROM orozova_datamart.dm_monthly_country_plan
WHERE plan NOT IN ('standard', 'plus', 'premium', 'metal', 'ultra')
UNION ALL
-- сверка с источником (основа ФТ-5): каждая витрина совпадает с ods,
-- поэтому после загрузки всех дней месяца комиссии в двух витринах совпадают между собой
SELECT 'fee_revenue: dm_daily = ods for ds', CASE WHEN (
    SELECT coalesce(sum(fee_revenue_eur), 0) FROM orozova_datamart.dm_daily_crypto_activity
    WHERE trade_date = %(ds)s
) = (
    SELECT coalesce(sum(fee_eur), 0) FROM orozova_ods.ods_trades
    WHERE trade_date = %(ds)s AND status = 'completed'
) THEN 0 ELSE 1 END
UNION ALL
SELECT 'fee_revenue: dm_monthly = ods for month of ds', CASE WHEN (
    SELECT coalesce(sum(fee_revenue_eur), 0) FROM orozova_datamart.dm_monthly_country_plan
    WHERE month = date_trunc('month', %(ds)s::date)::date
) = (
    SELECT coalesce(sum(fee_eur), 0) FROM orozova_ods.ods_trades
    WHERE date_trunc('month', trade_date) = date_trunc('month', %(ds)s::date) AND status = 'completed'
) THEN 0 ELSE 1 END;
