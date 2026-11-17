-- CDC (ФТ-9): «база сайта» — таблица orozova.users_kyc (источник для Debezium).
-- Запуск из папки project:  psql -h postgres -U orozova -d aiacademy -f sql/04_cdc.sql
-- Скрипт идемпотентный: повторный запуск не трогает уже существующие строки.

CREATE TABLE IF NOT EXISTS orozova.users_kyc (
    user_id     text        PRIMARY KEY,
    kyc_status  text        NOT NULL CHECK (kyc_status IN ('pending', 'verified', 'rejected')),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- updated_at обновляется сам при любом UPDATE — как в настоящем приложении
CREATE OR REPLACE FUNCTION orozova.users_kyc_touch() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS users_kyc_touch ON orozova.users_kyc;
CREATE TRIGGER users_kyc_touch BEFORE UPDATE ON orozova.users_kyc
    FOR EACH ROW EXECUTE FUNCTION orozova.users_kyc_touch();

-- Стартовые статусы из data/users_kyc_seed.csv (только новые строки)
CREATE TEMP TABLE kyc_seed (user_id text, kyc_status text, updated_at timestamptz);
\copy kyc_seed FROM 'data/users_kyc_seed.csv' WITH (FORMAT csv, HEADER true)
INSERT INTO orozova.users_kyc (user_id, kyc_status, updated_at)
SELECT user_id, kyc_status, updated_at FROM kyc_seed
ON CONFLICT (user_id) DO NOTHING;

SELECT kyc_status, count(*) FROM orozova.users_kyc GROUP BY 1 ORDER BY 1;
