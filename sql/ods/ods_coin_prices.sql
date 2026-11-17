-- stg_coingecko_prices → ods_coin_prices за дату ds. Параметр: %(ds)s

DELETE FROM orozova_ods.ods_coin_prices WHERE price_date = %(ds)s;

INSERT INTO orozova_ods.ods_coin_prices
    (coin_id, price_date, price_eur, market_cap_eur, volume_24h_eur, _loaded_at)
SELECT DISTINCT ON (coin_id)
       coin_id,
       snapshot_date::date,
       price_eur::numeric,
       nullif(nullif(market_cap_eur, ''), 'None')::numeric,
       nullif(nullif(volume_24h_eur, ''), 'None')::numeric,
       now()
FROM orozova_stg.stg_coingecko_prices
WHERE _ds = %(ds)s
ORDER BY coin_id, _loaded_at DESC
ON CONFLICT (coin_id, price_date) DO UPDATE SET
    price_eur = EXCLUDED.price_eur, market_cap_eur = EXCLUDED.market_cap_eur,
    volume_24h_eur = EXCLUDED.volume_24h_eur, _loaded_at = now();
