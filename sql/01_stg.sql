-- Слой stg: данные кладутся «как есть» — все бизнес-колонки text (ФТ-2),
-- плюс служебные колонки:
--   _ds           — логическая дата запуска DAG (за какой день загружено)
--   _source_file  — откуда пришла строка (имя файла или URL API)
--   _loaded_at    — когда строка записана
-- Идемпотентность (НФТ-1): загрузчик делает DELETE ... WHERE _ds = ds и INSERT
-- в одной транзакции, поэтому повтор за ту же дату не создаёт дублей.

-- Сделки (события): грузятся за дату ds из data/trades/trades_<ds>.csv
CREATE TABLE IF NOT EXISTS orozova_stg.stg_trades (
    trade_id      text,
    trade_ts      text,
    user_id       text,
    coin_id       text,
    side          text,
    amount_eur    text,
    fee_eur       text,
    status        text,
    _ds           date        NOT NULL,
    _source_file  text        NOT NULL,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS stg_trades_ds_idx ON orozova_stg.stg_trades (_ds);

-- Пользователи (справочник): грузится целиком при каждом запуске
CREATE TABLE IF NOT EXISTS orozova_stg.stg_users (
    user_id              text,
    country_code         text,
    signup_date          text,
    plan                 text,
    age_group            text,
    acquisition_channel  text,
    _ds           date        NOT NULL,
    _source_file  text        NOT NULL,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);

-- Ответ API CoinGecko /coins/{id}/history за дату ds: одна строка на монету
CREATE TABLE IF NOT EXISTS orozova_stg.stg_coingecko_prices (
    coin_id         text,
    snapshot_date   text,
    price_eur       text,
    market_cap_eur  text,
    volume_24h_eur  text,
    raw_json        text,   -- полный ответ API: пригодится, если схема ответа поменяется
    _ds           date        NOT NULL,
    _source_file  text        NOT NULL,
    _loaded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS stg_coingecko_prices_ds_idx ON orozova_stg.stg_coingecko_prices (_ds);

-- Журнал загрузок (ФТ-4): одна строка на каждый запуск каждой задачи
CREATE TABLE IF NOT EXISTS orozova_stg.etl_load_log (
    id           bigserial   PRIMARY KEY,
    dag_id       text        NOT NULL,
    task_id      text        NOT NULL,
    source       text        NOT NULL,              -- например trades.csv, coingecko_api
    ds           date        NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    status       text        NOT NULL DEFAULT 'running'
                 CHECK (status IN ('running', 'success', 'failed')),
    rows_loaded  integer,
    error_text   text
);
