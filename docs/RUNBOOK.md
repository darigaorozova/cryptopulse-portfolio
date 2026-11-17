# CryptoPulse — Runbook

Data platform for a crypto feature of a fictional neobank: 6 EU countries and a pilot in KG and KZ.
Files and the CoinGecko API are loaded on a schedule into PostgreSQL, pass through the stg → ods → datamart layers,
the mart is copied to ClickHouse, and KYC statuses arrive via CDC (Debezium + Kafka).

> Status: all stages are complete and verified on the test stand; history for 01.07–28.10.2026 is loaded (120 dates, all green).
> Quick start from scratch — see the "Setup from scratch" section at the end of this file.

## Structure

```
project/
├── dag/        Airflow DAGs (stage 2+)
├── data/       generator and CSV files
├── sql/        DDL and SQL for the layers
├── dbt/        dbt project (stage 5)
├── cdc/        Kafka Connect connector configs (stage 5)
├── dashboard/  template and HTML dashboard builder
├── docs/       dataset passport, data model, dashboard.html
└── README.md
```

## Stage 1: data and the stg layer

### 1. Generate the data

```bash
cd ~/work/dags/project/data
# with real CoinGecko prices (recommended; the key is not stored in any file):
COINGECKO_API_KEY=<your demo key> python3 generate_data.py
# or without a key, with synthetic prices:
python3 generate_data.py
```

Output: `users.csv`, `users_kyc_seed.csv`, `trades/trades_2026-07-01.csv … trades_2026-10-28.csv`.

### 2. Create the schemas and stg tables

```bash
psql -h <host> -U <user> -d aiacademy -f sql/00_schemas.sql
psql -h <host> -U <user> -d aiacademy -f sql/01_stg.sql
```

The password is entered interactively or read from `~/.pgpass`. No passwords are stored in the project files (NFR-2).

### 3. Check

```sql
\dt orozova_stg.*
-- stg_trades, stg_users, stg_coingecko_prices, etl_load_log
```

## Stage 2: loading DAG `final_orozova_pipeline`

File: `dag/final_orozova_pipeline.py`. For date ds, three tasks load data into stg,
and each writes a row to `orozova_stg.etl_load_log`.

| Task | Source | Table |
|---|---|---|
| `load_trades` | `data/trades/trades_<ds>.csv` | `orozova_stg.stg_trades` |
| `load_users` | `data/users.csv` (full file) | `orozova_stg.stg_users` |
| `load_prices` | CoinGecko `/coins/{id}/history`, 6 coins | `orozova_stg.stg_coingecko_prices` |

### 1. Airflow connections (Admin → Connections → +)

| Conn Id | Conn Type | Host | Schema | Login | Password | Port |
|---|---|---|---|---|---|---|
| `aiacademy_pg_orozova` | Postgres | `postgres` | `aiacademy` | `orozova` | database password | `5432` |
| `coingecko_api_orozova` | HTTP | `https://api.coingecko.com/api/v3` | — | — | CoinGecko demo key | — |

Passwords and the key live only here (NFR-2).

### 2. Run

The DAG is created paused. Turn it on with the toggle, then Trigger DAG → choose a Logical date
(for example 2026-09-15) → Trigger. The history for all 120 days is loaded at the end, once all layers are ready.

### 3. Check

```sql
-- row count equals the number of rows in the file for this date
select count(*) from orozova_stg.stg_trades where _ds = '2026-09-15';
-- load log
select * from orozova_stg.etl_load_log order by id desc limit 15;
```

API failure check (NFR-4): in the connection `coingecko_api_orozova`, change Host to
`https://api.coingecko.invalid/api/v3` and run a date. `load_prices` turns red after the retries,
no rows appear in `stg_coingecko_prices` for that date, and the log shows `failed`.

## Stage 3: ODS layer and quality checks

Tables: `sql/02_ods.sql` (run once):

```bash
psql -h postgres -U orozova -d aiacademy -f sql/02_ods.sql
```

After the stg load, the DAG runs these tasks (SQL in `sql/ods/`):

| Task | What it does |
|---|---|
| `ods_users` | reference table → `ods_users`, upsert on `user_id` |
| `ods_trades` | trades for ds → `ods_trades`: two date formats → timestamptz UTC; rows without an amount dropped; duplicate `trade_id` collapsed |
| `ods_coin_prices` | prices for ds → `ods_coin_prices`, key `(coin_id, price_date)` |
| `dq_checks` | 8 checks: unique and not_null keys, allowed `status`/`side`/`plan`, `amount_eur > 0`, every trade refers to an existing user, 6 prices per date |

FR-3 check: `\d orozova_ods.ods_trades` — dates are `timestamptz`/`date`, amounts are `numeric`, a primary key exists.

FR-10 check (break the data → red → fix → green):

```sql
update orozova_ods.ods_trades set status = 'weird'
where trade_id = (select min(trade_id) from orozova_ods.ods_trades where _ds = '2026-09-15');
```

Then Clear the `dq_checks` task for that date. It turns red with the message
`ods_trades: status in (completed, failed, cancelled): 1`. Clearing the `ods_trades` task reloads
the data from stg, and `dq_checks` turns green again.

## Stage 4: marts, mart DAG and ClickHouse

Mart tables and `ods_users_kyc` (run once):

```bash
psql -h postgres -U orozova -d aiacademy -f sql/03_datamart.sql
```

At the end, `final_orozova_pipeline` triggers `final_orozova_datamart` for the same logical date
(`trigger_datamart`, waits for completion). Mart DAG (`dag/final_orozova_datamart.py`):

| Task | What it does |
|---|---|
| `build_dm_daily` | `dm_daily_crypto_activity` for ds |
| `build_dm_monthly` | `dm_monthly_country_plan` for the whole month of ds |
| `dq_marts` | unique keys in the marts; fee reconciliation between the marts and ods |
| `copy_to_clickhouse` | daily mart for ds → ClickHouse `orozova.dm_daily_crypto_activity` (DROP PARTITION + INSERT) |
| `check_clickhouse` | row count and fee sum in ClickHouse = Postgres |
| `build_dashboard` | rebuilds `docs/dashboard.html` from the marts (after `dq_marts`) |

ClickHouse is accessed via the existing Airflow connection `orozova_clickhouse`.
Grain, keys and metric definitions are in `docs/data_model.md`.

FR-7 check (including after a repeated run):

```sql
-- ClickHouse
select sum(fee_revenue_eur) from orozova.dm_daily_crypto_activity;
-- Postgres
select sum(fee_revenue_eur) from orozova_datamart.dm_daily_crypto_activity;
```

## Stage 5a: dbt

Project: `dbt/` (profile `dbt/profiles.yml`; the password comes only from the environment variable `DBT_PASSWORD`).

| Model | Type | What it is |
|---|---|---|
| `v_trades`, `v_users`, `v_btc_prices` | view in `orozova_ods` | models over the ods sources (`source()`) |
| `dm_monthly_country_plan_dbt` | table in `orozova_datamart` | monthly mart built with `ref()` on the models above |

Tests (`dbt/models/marts/schema.yml`, `dbt/tests/`): `unique`/`not_null` on keys, `accepted_values`
for country and plan, and the test `assert_dbt_mart_matches_dag_mart`, which checks that the dbt mart
matches `dm_monthly_country_plan` from the DAG (FR-8).

In Airflow, dbt runs via the task `dbt_build` in the DAG `final_orozova_datamart` (the password comes from the connection).
Manually from a JupyterLab terminal:

```bash
cd ~/work/dags/project/dbt
export DBT_PASSWORD="$PGPASSWORD"   # in JupyterLab the database password is already in PGPASSWORD
dbt build --profiles-dir . -s +dm_monthly_country_plan_dbt   # "+" includes the upstream models it depends on
```

Expected result: `PASS=19 ERROR=0`. After the first run, `dbt build --profiles-dir . -s dm_monthly_country_plan_dbt` also works.

## Stage 5b: CDC — Debezium + Kafka (FR-9)

```
orozova.users_kyc ──WAL──► Debezium (users_kyc-source-orozova) ──► Kafka topic orozova.orozova.users_kyc
                                                                  └─► JDBC Sink (sink-orozova) ──upsert──► orozova_ods.ods_users_kyc
```

1. "Website database" and initial statuses (from the `project` folder):

```bash
psql -h postgres -U orozova -d aiacademy -f sql/04_cdc.sql
```

2. Register the connectors in Kafka Connect (`http://kafka-connect:8083`). The JSON files contain
`${PG_PASSWORD}` instead of a password; the script asks for the password and inserts it only into the request:

```bash
python cdc/register.py          # create/update connectors and show status
python cdc/register.py status   # check that both are RUNNING
```

3. Check (FR-9): `psql -h postgres -U orozova -d aiacademy -f cdc/demo.sql` changes the status of user
`U00042` and shows the row in the source and in `ods_users_kyc`; after a few seconds the value in ods matches.

## Dashboard

`docs/dashboard.html` is a single HTML page with charts built from the `orozova_datamart` marts
(active traders and BTC price by day, net flow, fees by month, country × month,
take rate by plan, country table). The data is embedded in the file, so it opens in any
browser without a server or database access.

It updates automatically: after each run, the task `build_dashboard` in the mart DAG reads both marts
in full and overwrites the file (atomically, via a temporary file). It runs only after
`dq_marts`, so data that failed the checks never reaches the dashboard. If the marts are empty,
the task fails and keeps the previous version.

Airflow runs as its own user, so the `docs/` folder needs write permission once
(otherwise you get `PermissionError`):
```bash
chmod a+w ~/work/dags/project/docs
```

Rebuild manually (from the project folder):
```bash
export PGPASSWORD=...        # database password; it is not written into the code
python dashboard/build_dashboard.py
```
To view: in Jupyter, open `docs/dashboard.html` → right-click → *Open in New Browser Tab*,
or download the file and open it by double-clicking.

## Setup from scratch (NFR-6)

All commands run in a JupyterLab terminal; the project is in `~/work/dags/project`.
The database password is already in the `PGPASSWORD` variable in JupyterLab.

1. **Data** (optional — the files are already in `data/`):
   `cd data && COINGECKO_API_KEY='<key>' python3 generate_data.py && cd ..`
2. **Schemas and tables:**
   ```bash
   for f in sql/00_schemas.sql sql/01_stg.sql sql/02_ods.sql sql/03_datamart.sql sql/04_cdc.sql; do
     psql -h postgres -U orozova -d aiacademy -f $f; done
   ```
3. **Airflow → Admin → Connections:** `aiacademy_pg_orozova` (Postgres: host `postgres`, db `aiacademy`,
   login `orozova`, database password, port 5432) and `coingecko_api_orozova` (HTTP: host
   `https://api.coingecko.com/api/v3`, password = demo key). `orozova_clickhouse` already exists on the stand.
4. **CDC:** `PG_PASSWORD="$PGPASSWORD" python cdc/register.py` → both connectors `RUNNING`.
5. **Run:** enable `orozova.final_orozova_pipeline` (the mart DAG is enabled automatically),
   Trigger DAG → Logical date (for example `2026-09-15`). The main DAG triggers the mart DAG itself.
6. **Check:** run the queries from the checklist below.

## Acceptance checklist

| No. | Check | How to check |
|---|---|---|
| FR-1 | dataset passport | `docs/dataset_passport.md` |
| FR-2 | rows in stg = rows in the file for the date | `select count(*) from orozova_stg.stg_trades where _ds='2026-09-15';` and `wc -l data/trades/trades_2026-09-15.csv` (minus the header) |
| FR-3 | types and PK in ods | `\d orozova_ods.ods_trades` |
| FR-4 | load log, including failed runs | `select * from orozova_stg.etl_load_log order by id desc limit 15;` |
| FR-5 | the metric sum over the month matches in both marts | the "Mart reconciliation" query in `docs/data_model.md` |
| FR-6 | the mart DAG ran automatically with the same date | Airflow → `orozova.final_orozova_datamart` → Runs |
| FR-7 | ClickHouse = Postgres, including after a repeated run | `select sum(fee_revenue_eur) from orozova.dm_daily_crypto_activity` in both databases |
| FR-8 | dbt model and tests are green | `cd dbt && export DBT_PASSWORD="$PGPASSWORD" && dbt build --profiles-dir . -s dm_monthly_country_plan_dbt` |
| FR-9 | CDC within ≤ 30 s, connectors RUNNING | `psql ... -f cdc/demo.sql`, then the query from the same file again; `python cdc/register.py status` |
| FR-10 | broken data → red check | see "Stage 3" (update status = 'weird' → Clear `dq_checks`) |
| NFR-1 | re-running a date does not change the row count | Clear the run for the date, compare `count(*)` in stg/ods/marts |
| NFR-2 | no passwords in the project | `grep -rin "password" .` — only comments and variable names, no values |
| NFR-4 | unavailable API → red task, empty stg | host `https://api.coingecko.invalid/api/v3` in `coingecko_api_orozova` |
| NFR-5 | one date in < 10 minutes | run duration in Airflow: ~30–60 s for both DAGs |
