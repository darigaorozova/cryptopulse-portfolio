-- Схемы проекта orozova в базе aiacademy (раздел 2 ТЗ, НФТ-3).
-- Запуск: psql -h <host> -U <user> -d aiacademy -f sql/00_schemas.sql
-- Скрипт идемпотентный: повторный запуск ничего не ломает.

CREATE SCHEMA IF NOT EXISTS orozova_stg;       -- сырые данные «как есть»
CREATE SCHEMA IF NOT EXISTS orozova_ods;       -- очищенные данные с типами и ключами
CREATE SCHEMA IF NOT EXISTS orozova_datamart;  -- витрины
CREATE SCHEMA IF NOT EXISTS orozova;           -- «база сайта» для CDC (users_kyc)
