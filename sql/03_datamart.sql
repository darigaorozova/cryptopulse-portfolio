-- Витрины (ФТ-5) и таблица статусов KYC в ods (её наполняет CDC, этап 5).
-- Определения метрик и зерно — в docs/data_model.md.

-- Статусы KYC: текущее состояние из «базы сайта» orozova.users_kyc (через Debezium + Kafka).
-- Таблица создаётся заранее, чтобы месячная витрина могла на неё ссылаться.
CREATE TABLE IF NOT EXISTS orozova_ods.ods_users_kyc (
    user_id     text        PRIMARY KEY,
    kyc_status  text        NOT NULL,      -- pending / verified / rejected
    updated_at  timestamptz
);

-- Дневная витрина. Зерно: один день (trade_date, UTC).
CREATE TABLE IF NOT EXISTS orozova_datamart.dm_daily_crypto_activity (
    trade_date        date           PRIMARY KEY,
    active_traders    integer        NOT NULL,   -- уникальные пользователи с завершённой сделкой
    trades_cnt        integer        NOT NULL,   -- все сделки дня (любой статус)
    completed_cnt     integer        NOT NULL,
    buy_volume_eur    numeric(16, 2) NOT NULL,
    sell_volume_eur   numeric(16, 2) NOT NULL,
    volume_eur        numeric(16, 2) NOT NULL,
    net_flow_eur      numeric(16, 2) NOT NULL,   -- покупки − продажи
    fee_revenue_eur   numeric(14, 2) NOT NULL,
    failed_share      numeric(6, 4)  NOT NULL,   -- доля сделок со статусом failed
    btc_price_eur     numeric(24, 10),           -- из API CoinGecko
    btc_change_pct    numeric(10, 4),            -- изменение цены BTC к прошлому дню, % (из API)
    _ds               date           NOT NULL,
    _built_at         timestamptz    NOT NULL DEFAULT now()
);

-- Месячная витрина. Зерно: месяц × страна × тариф.
CREATE TABLE IF NOT EXISTS orozova_datamart.dm_monthly_country_plan (
    month              date           NOT NULL,  -- первый день месяца
    country_code       char(2)        NOT NULL,
    plan               text           NOT NULL,
    active_traders     integer        NOT NULL,
    verified_users     integer        NOT NULL,  -- пользователи страны и тарифа с KYC verified
    conversion_to_trade numeric(6, 4),           -- active_traders / verified_users
    completed_cnt      integer        NOT NULL,
    volume_eur         numeric(16, 2) NOT NULL,
    fee_revenue_eur    numeric(14, 2) NOT NULL,
    take_rate          numeric(8, 6),            -- fee_revenue_eur / volume_eur
    avg_btc_price_eur  numeric(24, 10),          -- средняя цена BTC за дни месяца (из API)
    _ds                date           NOT NULL,
    _built_at          timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (month, country_code, plan)
);
