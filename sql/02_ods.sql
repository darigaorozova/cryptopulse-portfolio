-- Слой ods (ФТ-3): правильные типы, понятные имена, первичный ключ, без дублей.
-- Заполняется задачами DAG final_orozova_pipeline после загрузки stg
-- (SQL — в sql/ods/*.sql). Ограничения CHECK на статусы намеренно НЕ ставим:
-- допустимые значения проверяет задача качества (sql/ods/dq_checks.sql, ФТ-10),
-- чтобы «испорченные» данные было видно как красную проверку, а не как падение загрузки.

CREATE TABLE IF NOT EXISTS orozova_ods.ods_trades (
    trade_id     text           PRIMARY KEY,
    trade_ts     timestamptz    NOT NULL,       -- время сделки, UTC
    trade_date   date           NOT NULL,       -- дата сделки (UTC) — зерно дневной витрины
    user_id      text           NOT NULL,
    coin_id      text           NOT NULL,
    side         text           NOT NULL,       -- buy / sell
    amount_eur   numeric(14, 2) NOT NULL,
    fee_eur      numeric(10, 2) NOT NULL,
    status       text           NOT NULL,       -- completed / failed / cancelled
    _ds          date           NOT NULL,       -- за какую дату загружено
    _loaded_at   timestamptz    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ods_trades_ds_idx ON orozova_ods.ods_trades (_ds);
CREATE INDEX IF NOT EXISTS ods_trades_date_idx ON orozova_ods.ods_trades (trade_date);

CREATE TABLE IF NOT EXISTS orozova_ods.ods_users (
    user_id              text        PRIMARY KEY,
    country_code         char(2)     NOT NULL,
    signup_date          date        NOT NULL,
    plan                 text        NOT NULL,  -- standard / plus / premium / metal / ultra
    age_group            text,
    acquisition_channel  text,
    _loaded_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orozova_ods.ods_coin_prices (
    coin_id         text           NOT NULL,
    price_date      date           NOT NULL,     -- дата снимка CoinGecko (00:00 UTC)
    price_eur       numeric(24, 10) NOT NULL,
    market_cap_eur  numeric(28, 2),
    volume_24h_eur  numeric(28, 2),
    _loaded_at      timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (coin_id, price_date)
);
