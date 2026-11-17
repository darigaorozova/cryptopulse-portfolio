"""
final_orozova_pipeline — основной DAG проекта CryptoPulse (логин orozova).

За логическую дату ds:
  1. load_trades  — data/trades/trades_<ds>.csv        → orozova_stg.stg_trades   (ФТ-2)
  2. load_users   — data/users.csv (справочник целиком) → orozova_stg.stg_users    (ФТ-2)
  3. load_prices  — CoinGecko /coins/{id}/history за ds → orozova_stg.stg_coingecko_prices (ФТ-2, НФТ-4)
  4. ods_trades / ods_users / ods_coin_prices — stg → orozova_ods (типы, PK, без дублей; ФТ-3)
  5. dq_checks    — проверки качества ods (unique, not_null, допустимые значения; ФТ-10)
Каждая задача пишет строку в orozova_stg.etl_load_log (ФТ-4).

Идемпотентность (НФТ-1): каждая загрузка — одна транзакция «удалить за ds → вставить».
Секреты (НФТ-2): пароль БД — в Connection aiacademy_pg_orozova, ключ CoinGecko —
в Connection coingecko_api_orozova (поле Password). В коде паролей нет.
SQL слоя ods и проверок лежит в sql/ods/*.sql.

  6. trigger_datamart — запускает final_orozova_datamart за ту же логическую дату (ФТ-6)
"""

import csv
import io
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from _lib.tenant import student_dag

LOGIN = "orozova"
PG_CONN_ID = f"aiacademy_pg_{LOGIN}"
API_CONN_ID = f"coingecko_api_{LOGIN}"
DAG_ID = f"final_{LOGIN}_pipeline"

PROJECT_DIR = Path(__file__).resolve().parent.parent  # .../project
DATA_DIR = PROJECT_DIR / "data"
SQL_DIR = PROJECT_DIR / "sql"

COINS = ["bitcoin", "ethereum", "solana", "ripple", "cardano", "usd-coin"]
API_TIMEOUT = (5, 30)        # секунды: подключение, чтение
API_ATTEMPTS = 3             # повторы внутри задачи (поверх retries Airflow)
API_PAUSE = 1.5              # пауза между запросами: бережём лимит 100/мин

log = logging.getLogger("airflow.task")


# ---------------------------------------------------------------- helpers

def pg_conn():
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


@contextmanager
def etl_log(task_id, source, ds):
    """Пишет строку в журнал до начала работы и обновляет её в конце (ФТ-4).

    Журнал ведётся в отдельном подключении с autocommit: если основная
    транзакция откатится, запись о падении всё равно останется.
    """
    conn = pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {LOGIN}_stg.etl_load_log (dag_id, task_id, source, ds)
                VALUES (%s, %s, %s, %s) RETURNING id""",
            (DAG_ID, task_id, source, ds),
        )
        log_id = cur.fetchone()[0]
    result = {"rows": None}
    try:
        yield result
    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {LOGIN}_stg.etl_load_log
                    SET status = 'failed', finished_at = now(), error_text = %s
                    WHERE id = %s""",
                (f"{type(exc).__name__}: {exc}"[:4000], log_id),
            )
        conn.close()
        raise
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE {LOGIN}_stg.etl_load_log
                SET status = 'success', finished_at = now(), rows_loaded = %s
                WHERE id = %s""",
            (result["rows"], log_id),
        )
    conn.close()


def copy_csv_into(cur, table, columns, csv_path, ds, source_file):
    """Копирует CSV как есть (всё text) во временную таблицу и вставляет в stg
    со служебными колонками. Возвращает число вставленных строк."""
    cols = ", ".join(columns)
    cur.execute(
        f"CREATE TEMP TABLE tmp_load ({', '.join(c + ' text' for c in columns)}) ON COMMIT DROP"
    )
    with open(csv_path, encoding="utf-8") as f:
        header = next(csv.reader(f))
        if header != columns:
            raise ValueError(f"{csv_path.name}: ожидались колонки {columns}, пришли {header}")
        f.seek(0)
        cur.copy_expert(f"COPY tmp_load ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
    cur.execute(
        f"""INSERT INTO {table} ({cols}, _ds, _source_file, _loaded_at)
            SELECT {cols}, %s, %s, now() FROM tmp_load""",
        (ds, source_file),
    )
    return cur.rowcount


TRADE_COLS = ["trade_id", "trade_ts", "user_id", "coin_id", "side", "amount_eur", "fee_eur", "status"]
USER_COLS = ["user_id", "country_code", "signup_date", "plan", "age_group", "acquisition_channel"]


def load_trades_for(ds):
    csv_path = DATA_DIR / "trades" / f"trades_{ds}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"нет файла сделок за {ds}: {csv_path}")
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LOGIN}_stg.stg_trades WHERE _ds = %s", (ds,))
            rows = copy_csv_into(cur, f"{LOGIN}_stg.stg_trades", TRADE_COLS, csv_path, ds, csv_path.name)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return rows


def load_users_for(ds):
    csv_path = DATA_DIR / "users.csv"
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {LOGIN}_stg.stg_users")  # справочник целиком
            rows = copy_csv_into(cur, f"{LOGIN}_stg.stg_users", USER_COLS, csv_path, ds, csv_path.name)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return rows


def fetch_coin(session, base_url, coin, ds):
    """Один запрос к API с таймаутом и повторами; 429 и 5xx — повторяем с паузой."""
    url = f"{base_url.rstrip('/')}/coins/{coin}/history"
    params = {"date": datetime.strptime(ds, "%Y-%m-%d").strftime("%d-%m-%Y"), "localization": "false"}
    last_error = None
    for attempt in range(1, API_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=API_TIMEOUT)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            payload = resp.json()
            market = payload.get("market_data")
            if not market or "eur" not in market.get("current_price", {}):
                raise ValueError(f"{coin}: в ответе нет market_data.current_price.eur")
            return url, payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            wait = 2 ** attempt
            log.warning("%s: попытка %d/%d не удалась (%s), жду %d с", coin, attempt, API_ATTEMPTS, exc, wait)
            if attempt < API_ATTEMPTS:
                time.sleep(wait)
    raise RuntimeError(f"CoinGecko недоступен для {coin} за {ds}: {last_error}")


def load_prices_for(ds):
    api = BaseHook.get_connection(API_CONN_ID)
    base_url = api.host or "https://api.coingecko.com/api/v3"
    if not base_url.startswith("http"):
        base_url = f"{api.schema or 'https'}://{base_url}"
    session = requests.Session()
    session.headers.update({"x-cg-demo-api-key": api.password, "accept": "application/json"})

    # Сначала получаем ВСЕ монеты. Если хоть одна не пришла — падаем,
    # ничего не записав: частичных данных в stg не бывает (НФТ-4).
    records = []
    for coin in COINS:
        url, payload = fetch_coin(session, base_url, coin, ds)
        m = payload["market_data"]
        records.append((
            coin,
            ds,
            str(m["current_price"]["eur"]),
            str(m.get("market_cap", {}).get("eur")),
            str(m.get("total_volume", {}).get("eur")),
            json.dumps(payload, ensure_ascii=False),
            ds,
            url,
        ))
        time.sleep(API_PAUSE)

    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LOGIN}_stg.stg_coingecko_prices WHERE _ds = %s", (ds,))
            cur.executemany(
                f"""INSERT INTO {LOGIN}_stg.stg_coingecko_prices
                    (coin_id, snapshot_date, price_eur, market_cap_eur, volume_24h_eur,
                     raw_json, _ds, _source_file, _loaded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
                records,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(records)


def run_sql_file(relative_path, ds):
    """Выполняет SQL-файл одной транзакцией. Возвращает число строк последней команды."""
    sql = (SQL_DIR / relative_path).read_text(encoding="utf-8")
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"ds": ds})
            rows = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return rows


def run_dq_checks(ds):
    """Проверки качества (ФТ-10): падаем, если хоть одна проверка нашла нарушения."""
    sql = (SQL_DIR / "ods" / "dq_checks.sql").read_text(encoding="utf-8")
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"ds": ds})
            results = cur.fetchall()
    finally:
        conn.close()
    failed = []
    for name, violations in results:
        mark = "OK  " if violations == 0 else "FAIL"
        log.info("%s %s — нарушений: %s", mark, name, violations)
        if violations:
            failed.append(f"{name}: {violations}")
    if failed:
        raise ValueError("Проверки качества не пройдены: " + "; ".join(failed))
    return len(results)


# ---------------------------------------------------------------- DAG

with student_dag(
    __file__,
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 10, 28, tz="UTC"),
    schedule="@daily",
    catchup=False,  # история догружается отдельно, когда весь пайплайн готов
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=[LOGIN, "final"],
) as dag:

    @task
    def load_trades(ds=None):
        with etl_log("load_trades", "trades.csv", ds) as res:
            res["rows"] = load_trades_for(ds)
            log.info("stg_trades за %s: %s строк", ds, res["rows"])
        return res["rows"]

    @task
    def load_users(ds=None):
        with etl_log("load_users", "users.csv", ds) as res:
            res["rows"] = load_users_for(ds)
            log.info("stg_users: %s строк", res["rows"])
        return res["rows"]

    @task
    def load_prices(ds=None):
        with etl_log("load_prices", "coingecko_api", ds) as res:
            res["rows"] = load_prices_for(ds)
            log.info("stg_coingecko_prices за %s: %s строк", ds, res["rows"])
        return res["rows"]

    @task
    def ods_users(ds=None):
        with etl_log("ods_users", "stg_users", ds) as res:
            res["rows"] = run_sql_file("ods/ods_users.sql", ds)
            log.info("ods_users: %s строк", res["rows"])
        return res["rows"]

    @task
    def ods_trades(ds=None):
        with etl_log("ods_trades", "stg_trades", ds) as res:
            res["rows"] = run_sql_file("ods/ods_trades.sql", ds)
            log.info("ods_trades за %s: %s строк (после очистки и дедупликации)", ds, res["rows"])
        return res["rows"]

    @task
    def ods_coin_prices(ds=None):
        with etl_log("ods_coin_prices", "stg_coingecko_prices", ds) as res:
            res["rows"] = run_sql_file("ods/ods_coin_prices.sql", ds)
            log.info("ods_coin_prices за %s: %s строк", ds, res["rows"])
        return res["rows"]

    @task
    def dq_checks(ds=None):
        with etl_log("dq_checks", "orozova_ods", ds) as res:
            res["rows"] = run_dq_checks(ds)
        return res["rows"]

    t_users, t_trades, t_prices = load_users(), load_trades(), load_prices()
    o_users, o_trades, o_prices = ods_users(), ods_trades(), ods_coin_prices()

    t_users >> o_users
    t_trades >> o_trades
    t_prices >> o_prices
    o_users >> o_trades          # проверка «пользователь существует» требует свежий справочник
    # Обёртка курса добавляет к dag_id префикс (orozova.final_orozova_pipeline),
    # поэтому id DAG витрин берём от фактического id этого DAG.
    trigger_datamart = TriggerDagRunOperator(
        task_id="trigger_datamart",
        trigger_dag_id=dag.dag_id.replace("pipeline", "datamart"),
        logical_date="{{ logical_date }}",   # та же логическая дата
        reset_dag_run=True,                  # повторный запуск за дату пересчитывает витрины
        wait_for_completion=True,            # основной DAG зелёный, только если витрины зелёные
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    [o_users, o_trades, o_prices] >> dq_checks() >> trigger_datamart
