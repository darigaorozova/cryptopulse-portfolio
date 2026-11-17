"""
Сборка HTML-дашборда CryptoPulse из витрин orozova_datamart.

Берёт все строки dm_daily_crypto_activity и dm_monthly_country_plan, вшивает их как JSON
в dashboard/template.html и пишет docs/dashboard.html. Файл открывается в браузере без сервера.

Вызывается:
  * из DAG final_orozova_datamart (задача build_dashboard) — после проверок dq_marts,
    поэтому на дашборд попадают только проверенные данные;
  * вручную из папки project:  python dashboard/build_dashboard.py
    (подключение из переменных PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE, пароль в код не пишется — НФТ-2).

Запись атомарная: сначала временный файл, потом os.replace — открытый в браузере
дашборд никогда не окажется «наполовину записанным».
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "template.html"
DEFAULT_OUT = HERE.parent / "docs" / "dashboard.html"
SCHEMA = "orozova_datamart"
PLACEHOLDER = "/*DATA*/"

DAILY_SQL = f"""
SELECT trade_date::text, active_traders, trades_cnt, completed_cnt, volume_eur,
       net_flow_eur, fee_revenue_eur, failed_share, btc_price_eur, btc_change_pct
FROM {SCHEMA}.dm_daily_crypto_activity
ORDER BY trade_date
"""

MONTHLY_SQL = f"""
SELECT to_char(month, 'YYYY-MM'), country_code, plan, active_traders,
       completed_cnt, volume_eur, fee_revenue_eur
FROM {SCHEMA}.dm_monthly_country_plan
ORDER BY month, country_code, plan
"""


def _f(v, nd=2):
    return None if v is None else round(float(v), nd)


def fetch(conn):
    with conn.cursor() as cur:
        cur.execute(DAILY_SQL)
        daily_rows = cur.fetchall()
        cur.execute(MONTHLY_SQL)
        monthly_rows = cur.fetchall()

    daily = [
        {"d": d, "t": int(t), "all": int(a), "n": int(n), "vol": _f(vol), "net": _f(net),
         "fee": _f(fee), "fail": _f(fail, 4), "btc": _f(btc), "ch": _f(ch, 4)}
        for d, t, a, n, vol, net, fee, fail, btc, ch in daily_rows
    ]
    # Если цены за какой-то день нет, переносим последнюю известную (только для графика)
    known = [r["btc"] for r in daily if r["btc"] is not None]
    prev = known[0] if known else None
    for r in daily:
        if r["btc"] is None:
            r["btc"] = prev
        prev = r["btc"]

    monthly = [
        {"m": m, "c": c, "p": p, "at": int(at), "n": int(n), "vol": _f(vol), "fee": _f(fee)}
        for m, c, p, at, n, vol, fee in monthly_rows
    ]
    return daily, monthly


def render(conn, out_path=DEFAULT_OUT):
    """Читает витрины через открытое соединение psycopg2 и пишет HTML. Возвращает число дней."""
    daily, monthly = fetch(conn)
    if not daily or not monthly or daily[0]["btc"] is None:
        raise ValueError("Витрины пустые — дашборд не пересобран, старая версия оставлена")

    built_at = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    payload = {"meta": {"built_at": built_at, "last_ds": daily[-1]["d"]},
               "daily": daily, "monthly": monthly}
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")

    template = TEMPLATE.read_text(encoding="utf-8")
    if template.count(PLACEHOLDER) != 1:
        raise ValueError(f"В шаблоне должна быть ровно одна метка {PLACEHOLDER}")
    html = template.replace(PLACEHOLDER, data)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=out_path.parent, prefix=".dashboard_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(html)
        os.chmod(tmp, 0o644)
        os.replace(tmp, out_path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return len(daily), len(monthly), str(out_path)


if __name__ == "__main__":
    import psycopg2

    if not os.environ.get("PGPASSWORD"):
        sys.exit("Нужна переменная PGPASSWORD (export PGPASSWORD=...), пароль в код не пишем.")
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=int(os.environ.get("PGPORT", 5432)),
        user=os.environ.get("PGUSER", "orozova"),
        dbname=os.environ.get("PGDATABASE", "aiacademy"),
    )
    try:
        days, rows, path = render(conn, sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUT)
    finally:
        conn.close()
    print(f"Дашборд собран: {days} дней, {rows} строк месячной витрины → {path}")
