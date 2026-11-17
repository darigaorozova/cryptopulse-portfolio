-- stg_trades → ods_trades за дату ds. Параметр: %(ds)s
-- 1) даты в двух форматов приводятся к timestamptz (UTC);
-- 2) строки без суммы отбрасываются (невалидная сделка);
-- 3) дубли trade_id схлопываются (DISTINCT ON, берём последнюю загруженную);
-- 4) идемпотентно: удаляем данные за ds и вставляем заново, ON CONFLICT — страховка.

DELETE FROM orozova_ods.ods_trades WHERE _ds = %(ds)s;

INSERT INTO orozova_ods.ods_trades
    (trade_id, trade_ts, trade_date, user_id, coin_id, side, amount_eur, fee_eur, status, _ds, _loaded_at)
SELECT trade_id, ts, (ts AT TIME ZONE 'UTC')::date, user_id, coin_id, side,
       amount_eur, fee_eur, status, %(ds)s, now()
FROM (
    SELECT DISTINCT ON (trim(trade_id))
           trim(trade_id)                                   AS trade_id,
           CASE
               WHEN trade_ts ~ '^\d{4}-\d{2}-\d{2}'
                   THEN trade_ts::timestamptz
               WHEN trade_ts ~ '^\d{2}\.\d{2}\.\d{4}'
                   THEN to_timestamp(trade_ts, 'DD.MM.YYYY HH24:MI:SS')::timestamp AT TIME ZONE 'UTC'
           END                                              AS ts,
           trim(user_id)                                    AS user_id,
           lower(trim(coin_id))                             AS coin_id,
           lower(trim(side))                                AS side,
           nullif(trim(amount_eur), '')::numeric(14, 2)     AS amount_eur,
           coalesce(nullif(trim(fee_eur), '')::numeric(10, 2), 0) AS fee_eur,
           lower(trim(status))                              AS status
    FROM orozova_stg.stg_trades
    WHERE _ds = %(ds)s
      AND nullif(trim(trade_id), '') IS NOT NULL
    ORDER BY trim(trade_id), _loaded_at DESC
) t
WHERE ts IS NOT NULL
  AND amount_eur IS NOT NULL
ON CONFLICT (trade_id) DO UPDATE SET
    trade_ts = EXCLUDED.trade_ts, trade_date = EXCLUDED.trade_date, user_id = EXCLUDED.user_id,
    coin_id = EXCLUDED.coin_id, side = EXCLUDED.side, amount_eur = EXCLUDED.amount_eur,
    fee_eur = EXCLUDED.fee_eur, status = EXCLUDED.status, _ds = EXCLUDED._ds, _loaded_at = now();
