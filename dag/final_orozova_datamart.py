"""
final_orozova_datamart — DAG витрин проекта CryptoPulse (логин orozova).

Запускается из final_orozova_pipeline (TriggerDagRunOperator) за ту же логическую дату (ФТ-6).
  1. build_dm_daily     — ods → orozova_datamart.dm_daily_crypto_activity (зерно: день)            (ФТ-5)
  2. build_dm_monthly   — ods → orozova_datamart.dm_monthly_country_plan (месяц × страна × тариф)  (ФТ-5)
  3. dq_marts           — уникальность ключей витрин и сверка комиссий витрин с ods             (ФТ-10, ФТ-5)
  4. dbt_build          — dbt-модель dm_monthly_country_plan_dbt + тесты (dbt/)               (ФТ-8)
  5. copy_to_clickhouse — дневная витрина за ds → ClickHouse orozova.dm_daily_crypto_activity   (ФТ-7)
  6. check_clickhouse   — суммы в ClickHouse и Postgres совпадают, дублей нет                   (ФТ-7)
  7. build_dashboard    — пересобирает docs/dashboard.html из витрин (только после dq_marts)
Все задачи пишут в orozova_stg.etl_load_log (ФТ-4). Пересчёт идемпотентен (НФТ-1).
ClickHouse — через существующее подключение Airflow orozova_clickhouse (пароль только там).
"""

import logging
import os
import shutil
import subprocess
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from _lib.tenant import student_dag

LOGIN = "orozova"
PG_CONN_ID = f"aiacademy_pg_{LOGIN}"
CH_CONN_ID = f"{LOGIN}_clickhouse"
CH_DB = LOGIN
DAG_ID = f"final_{LOGIN}_datamart"

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"
DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"
DASHBOARD_OUT = Path(__file__).resolve().parent.parent / "docs" / "dashboard.html"
log = logging.getLogger("airflow.task")

DAILY_COLS = [
    "trade_date", "active_traders", "trades_cnt", "completed_cnt", "buy_volume_eur",
    "sell_volume_eur", "volume_eur", "net_flow_eur", "fee_revenue_eur", "failed_share",
    "btc_price_eur", "btc_change_pct", "_ds",
]

CH_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.dm_daily_crypto_activity
(
    trade_date       Date,
    active_traders   UInt32,
    trades_cnt       UInt32,
    completed_cnt    UInt32,
    buy_volume_eur   Decimal(16, 2),
    sell_volume_eur  Decimal(16, 2),
    volume_eur       Decimal(16, 2),
    net_flow_eur     Decimal(16, 2),
    fee_revenue_eur  Decimal(14, 2),
    failed_share     Decimal(6, 4),
    btc_price_eur    Nullable(Decimal(24, 10)),
    btc_change_pct   Nullable(Decimal(10, 4)),
    _ds              Date,
    _loaded_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY trade_date      -- одна партиция на день: перезапуск за дату = DROP PARTITION + INSERT
ORDER BY trade_date
"""


def pg_conn():
    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


def ch_client():
    import clickhouse_connect

    c = BaseHook.get_connection(CH_CONN_ID)
    client = clickhouse_connect.get_client(
        host=c.host, port=c.port or 8123, username=c.login, password=c.password
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
    client.command(CH_DDL)
    return client


@contextmanager
def etl_log(task_id, source, ds):
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
                    SET status = 'failed', finished_at = now(), error_text = %s WHERE id = %s""",
                (f"{type(exc).__name__}: {exc}"[:4000], log_id),
            )
        conn.close()
        raise
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE {LOGIN}_stg.etl_load_log
                SET status = 'success', finished_at = now(), rows_loaded = %s WHERE id = %s""",
            (result["rows"], log_id),
        )
    conn.close()


def run_sql_file(relative_path, ds):
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


def run_checks(relative_path, ds):
    sql = (SQL_DIR / relative_path).read_text(encoding="utf-8")
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"ds": ds})
            results = cur.fetchall()
    finally:
        conn.close()
    failed = []
    for name, violations in results:
        log.info("%s %s — нарушений: %s", "OK  " if violations == 0 else "FAIL", name, violations)
        if violations:
            failed.append(f"{name}: {violations}")
    if failed:
        raise ValueError("Проверки качества не пройдены: " + "; ".join(failed))
    return len(results)


def copy_daily_to_clickhouse(ds):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(DAILY_COLS)} FROM {LOGIN}_datamart.dm_daily_crypto_activity "
                "WHERE trade_date = %s",
                (ds,),
            )
            rows = [list(r) for r in cur.fetchall()]
    finally:
        conn.close()

    client = ch_client()
    try:
        # идемпотентно: партиция дня удаляется и записывается заново (как в pgtoClick из курса)
        client.command(f"ALTER TABLE {CH_DB}.dm_daily_crypto_activity DROP PARTITION '{ds}'")
        if rows:
            client.insert(f"{CH_DB}.dm_daily_crypto_activity", rows, column_names=DAILY_COLS)
    finally:
        client.close()
    log.info("ClickHouse %s.dm_daily_crypto_activity за %s: %s строк", CH_DB, ds, len(rows))
    return len(rows)


def check_clickhouse(ds):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*), coalesce(sum(fee_revenue_eur), 0) "
                f"FROM {LOGIN}_datamart.dm_daily_crypto_activity WHERE trade_date = %s",
                (ds,),
            )
            pg_cnt, pg_fee = cur.fetchone()
    finally:
        conn.close()
    client = ch_client()
    try:
        ch_cnt, ch_fee = client.query(
            f"SELECT count(), sum(fee_revenue_eur) FROM {CH_DB}.dm_daily_crypto_activity "
            f"WHERE trade_date = '{ds}'"
        ).result_rows[0]
    finally:
        client.close()
    log.info("за %s: Postgres count=%s fee=%s | ClickHouse count=%s fee=%s", ds, pg_cnt, pg_fee, ch_cnt, ch_fee)
    if pg_cnt != ch_cnt or round(float(pg_fee), 2) != round(float(ch_fee or 0), 2):
        raise ValueError(f"Расхождение за {ds}: PG count={pg_cnt}, fee={pg_fee}; CH count={ch_cnt}, fee={ch_fee}")
    return ch_cnt


def run_dbt_build():
    """dbt build по проекту dbt/. Пароль передаётся только через переменную окружения
    процесса dbt (из Connection), в файлы и в шаблоны Airflow он не попадает (НФТ-2)."""
    c = BaseHook.get_connection(PG_CONN_ID)
    env = dict(os.environ)
    env.update({
        "DBT_HOST": c.host or "postgres",
        "DBT_PORT": str(c.port or 5432),
        "DBT_USER": c.login,
        "DBT_PASSWORD": c.password,
        "DBT_TARGET_PATH": f"/tmp/dbt_target_{LOGIN}",
        "DBT_LOG_PATH": f"/tmp/dbt_logs_{LOGIN}",
    })
    dbt_bin = shutil.which("dbt") or "dbt"
    cmd = [dbt_bin, "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)
    for line in (proc.stdout + proc.stderr).splitlines():
        log.info(line)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt build завершился с кодом {proc.returncode}")
    return 0


def build_dashboard_html():
    """Пересобирает HTML-дашборд из витрин Postgres (dashboard/build_dashboard.py)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("build_dashboard", DASHBOARD_DIR / "build_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    conn = pg_conn()
    try:
        days, rows, path = module.render(conn, DASHBOARD_OUT)
    finally:
        conn.close()
    log.info("Дашборд пересобран: %s дней, %s строк месячной витрины → %s", days, rows, path)
    return days


with student_dag(
    __file__,
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    schedule=None,                    # запускается только основным DAG
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,    # иначе запуски из основного DAG будут ждать в очереди
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=[LOGIN, "final"],
) as dag:

    @task
    def build_dm_daily(ds=None):
        with etl_log("build_dm_daily", "orozova_ods", ds) as res:
            res["rows"] = run_sql_file("datamart/dm_daily.sql", ds)
        return res["rows"]

    @task
    def build_dm_monthly(ds=None):
        with etl_log("build_dm_monthly", "orozova_ods", ds) as res:
            res["rows"] = run_sql_file("datamart/dm_monthly.sql", ds)
        return res["rows"]

    @task
    def dq_marts(ds=None):
        with etl_log("dq_marts", "orozova_datamart", ds) as res:
            res["rows"] = run_checks("datamart/dq_marts.sql", ds)
        return res["rows"]

    @task
    def copy_to_clickhouse(ds=None):
        with etl_log("copy_to_clickhouse", "dm_daily_crypto_activity", ds) as res:
            res["rows"] = copy_daily_to_clickhouse(ds)
        return res["rows"]

    @task
    def check_clickhouse_task(ds=None):
        with etl_log("check_clickhouse", "clickhouse", ds) as res:
            res["rows"] = check_clickhouse(ds)
        return res["rows"]

    @task
    def dbt_build(ds=None):
        with etl_log("dbt_build", "dbt/dm_monthly_country_plan_dbt", ds) as res:
            res["rows"] = run_dbt_build()
        return res["rows"]

    @task
    def build_dashboard(ds=None):
        with etl_log("build_dashboard", "orozova_datamart", ds) as res:
            res["rows"] = build_dashboard_html()
        return res["rows"]

    checks = dq_marts()
    [build_dm_daily(), build_dm_monthly()] >> checks
    checks >> dbt_build()
    checks >> copy_to_clickhouse() >> check_clickhouse_task()
    checks >> build_dashboard()
