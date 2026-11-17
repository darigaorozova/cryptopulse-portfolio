# Data Model — CryptoPulse

Storage in PostgreSQL (database `aiacademy`): three layers, plus a copy of the daily mart in ClickHouse.

```
CSV (trades, users) ─┐
CoinGecko API ───────┼─► orozova_stg ─► orozova_ods ─► orozova_datamart ─► ClickHouse orozova.*
orozova.users_kyc ───┘   (as is)        (types, PK)    (marts)            (copy of the daily mart)
   └──── CDC: Debezium → Kafka ────────► orozova_ods.ods_users_kyc
```

DAG `final_orozova_pipeline` (stg → ods → checks) triggers `final_orozova_datamart`
(marts → checks → ClickHouse) for the same logical date.

## Staging layer — `orozova_stg`

Data as received: all business columns are `text`, plus the service columns `_ds`, `_source_file`, `_loaded_at`.

| Table | Grain | Logical key | Source |
|---|---|---|---|
| `stg_trades` | one row of the trades file for `_ds` (duplicates possible) | — | `data/trades/trades_<ds>.csv` |
| `stg_users` | one row of the reference table | — | `data/users.csv` (full file) |
| `stg_coingecko_prices` | one coin for `_ds` | — | CoinGecko `/coins/{id}/history` |
| `etl_load_log` | one run of one task | `id` | all tasks in both DAGs |

## ODS layer — `orozova_ods`

| Table | Grain | Primary key | What is done |
|---|---|---|---|
| `ods_trades` | one trade | `trade_id` | dates → `timestamptz` UTC and `trade_date`; amounts → `numeric`; duplicates collapsed; rows without an amount dropped |
| `ods_users` | one user | `user_id` | types; case normalization |
| `ods_coin_prices` | coin × day | `(coin_id, price_date)` | prices → `numeric` |
| `ods_users_kyc` | one user (current state) | `user_id` | filled by CDC from `orozova.users_kyc` |

## Marts — `orozova_datamart`

All volume and revenue metrics include **only trades with status `completed`**.

### `dm_daily_crypto_activity` — grain: one day (`trade_date`, UTC)

| Column | Definition |
|---|---|
| `trade_date` (PK) | trade date, UTC |
| `active_traders` | number of distinct `user_id` with a completed trade that day |
| `trades_cnt` | all trades of the day, any status |
| `completed_cnt` | trades with status `completed` |
| `buy_volume_eur` / `sell_volume_eur` | sum of `amount_eur` for buys / sells |
| `volume_eur` | sum of `amount_eur` |
| `net_flow_eur` | buys − sells |
| `fee_revenue_eur` | sum of `fee_eur`: the bank's fee revenue |
| `failed_share` | share of trades with status `failed` among all trades of the day |
| `btc_price_eur` | BTC price at 00:00 UTC of the day, **from the CoinGecko API** |
| `btc_change_pct` | change of the BTC price versus the previous day, %, **from the CoinGecko API** (empty if the previous day was not loaded) |

### `dm_monthly_country_plan` — grain: month × country × plan

| Column | Definition |
|---|---|
| `month`, `country_code`, `plan` (PK) | first day of the month; user's country; plan |
| `active_traders` | distinct users with a completed trade in the month (MAU) |
| `verified_users` | users of this country and plan with KYC status `verified` (from CDC) |
| `conversion_to_trade` | `active_traders / verified_users` |
| `completed_cnt` | completed trades |
| `volume_eur` | sum of `amount_eur` |
| `fee_revenue_eur` | sum of `fee_eur` |
| `take_rate` | `fee_revenue_eur / volume_eur` |
| `avg_btc_price_eur` | average BTC price over the loaded days of the month, **from the CoinGecko API** |

On every run the monthly mart recalculates the whole month that contains `ds`.

## Questions the marts answer in one query, without JOINs

```sql
-- 1. Which September day had the most active traders, and how did BTC move that day?
select trade_date, active_traders, btc_change_pct
from orozova_datamart.dm_daily_crypto_activity
where trade_date between '2026-09-01' and '2026-09-30'
order by active_traders desc limit 1;

-- 2. Which country generated the most fee revenue in September? What does the KG/KZ pilot look like?
select country_code, sum(fee_revenue_eur) as fee_eur, sum(active_traders) as traders
from orozova_datamart.dm_monthly_country_plan
where month = '2026-09-01'
group by country_code order by fee_eur desc;

-- 3. Which plan has the highest conversion from verified user to active trader?
select plan, round(sum(active_traders)::numeric / nullif(sum(verified_users), 0), 4) as conversion
from orozova_datamart.dm_monthly_country_plan
where month = '2026-09-01'
group by plan order by conversion desc;
```

## Mart reconciliation (FR-5)

Monthly fee revenue matches in both marts (after all days of the month are loaded):

```sql
select
  (select sum(fee_revenue_eur) from orozova_datamart.dm_daily_crypto_activity
   where trade_date between '2026-09-01' and '2026-09-30') as daily_sum,
  (select sum(fee_revenue_eur) from orozova_datamart.dm_monthly_country_plan
   where month = '2026-09-01') as monthly_sum;
```

## ClickHouse

`orozova.dm_daily_crypto_activity` is a copy of the daily mart. `ENGINE = MergeTree`, `PARTITION BY trade_date`:
a re-run for a date drops that date's partition and writes it again, so there are no duplicates.
The task `check_clickhouse` compares the row count and the sum of `fee_revenue_eur` with Postgres.
