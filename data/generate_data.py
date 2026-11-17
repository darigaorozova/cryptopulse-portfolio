"""
Генератор синтетических данных для проекта CryptoPulse (логин orozova).

Что создаёт (рядом с этим скриптом):
  users.csv                      — справочник пользователей (грузится целиком)
  trades/trades_YYYY-MM-DD.csv   — сделки, один файл на день (грузится за дату ds)
  users_kyc_seed.csv             — начальные статусы KYC для таблицы orozova.users_kyc (CDC)

Цены нужны генератору только чтобы «оживить» активность: в дни сильного
движения BTC сделок больше, на падении чаще покупают. Источник цен:
  * с ключом CoinGecko (переменная окружения COINGECKO_API_KEY) — реальные
    дневные цены через /coins/{id}/market_chart/range (6 запросов на весь период);
  * без ключа — условная траектория (случайное блуждание), помечается в выводе.

Запуск:
  python3 generate_data.py                         # без ключа
  COINGECKO_API_KEY=... python3 generate_data.py   # с реальными ценами

Ключ не хранится в коде и в файлах (НФТ-2).
"""

import csv
import json
import math
import os
import random
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SEED = 18  # номер источника — данные воспроизводимы
START = date(2026, 7, 1)
END = date(2026, 10, 28)
PILOT_START = date(2026, 9, 1)  # запуск пилота в KG и KZ
N_USERS = 3000

OUT = Path(__file__).resolve().parent

COINS = {  # coin_id CoinGecko: (доля в сделках, условная стартовая цена EUR)
    "bitcoin": (0.40, 95000.0),
    "ethereum": (0.25, 3300.0),
    "solana": (0.12, 160.0),
    "ripple": (0.10, 2.1),
    "cardano": (0.06, 0.65),
    "usd-coin": (0.07, 0.86),
}
COUNTRIES = {"DE": 0.24, "FR": 0.18, "ES": 0.14, "IT": 0.13, "NL": 0.09, "PL": 0.10, "KG": 0.06, "KZ": 0.06}
PILOT = {"KG", "KZ"}
PLANS = {"standard": 0.55, "plus": 0.15, "premium": 0.15, "metal": 0.10, "ultra": 0.05}
FEE_RATE = {"standard": 0.0149, "plus": 0.0149, "premium": 0.0099, "metal": 0.0099, "ultra": 0.0049}
MIN_FEE = 0.99
AMOUNT_MULT = {"standard": 1.0, "plus": 1.2, "premium": 1.8, "metal": 2.6, "ultra": 4.0}
AGE_GROUPS = {"18-24": 0.20, "25-34": 0.36, "35-44": 0.24, "45-54": 0.13, "55+": 0.07}
CHANNELS = {"organic": 0.45, "referral": 0.25, "paid_social": 0.20, "partner": 0.10}
KYC = {"verified": 0.85, "pending": 0.10, "rejected": 0.05}
STATUSES = {"completed": 0.95, "failed": 0.035, "cancelled": 0.015}


def pick(rng, weights):
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def days(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fetch_real_prices(api_key):
    """Реальные дневные цены в EUR: {coin_id: {date: price}}. 6 запросов к API."""
    frm = datetime.combine(START - timedelta(days=1), datetime.min.time(), timezone.utc)
    to = datetime.combine(END + timedelta(days=1), datetime.min.time(), timezone.utc)
    prices = {}
    for coin in COINS:
        url = (
            f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range"
            f"?vs_currency=eur&from={int(frm.timestamp())}&to={int(to.timestamp())}"
        )
        req = urllib.request.Request(url, headers={"x-cg-demo-api-key": api_key})
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.load(resp)
        series = {}
        for ts_ms, price in payload["prices"]:
            series[datetime.fromtimestamp(ts_ms / 1000, timezone.utc).date()] = price
        prices[coin] = series
    return prices


def synthetic_prices(rng):
    prices = {}
    for coin, (_, start_price) in COINS.items():
        vol = 0.002 if coin == "usd-coin" else (0.03 if coin == "bitcoin" else 0.045)
        p, series = start_price, {}
        for d in days(START - timedelta(days=1), END):
            p *= math.exp(rng.gauss(0, vol))
            series[d] = p
        prices[coin] = series
    return prices


def main():
    rng = random.Random(SEED)

    api_key = os.environ.get("COINGECKO_API_KEY")
    if api_key:
        prices = fetch_real_prices(api_key)
        price_source = "CoinGecko (реальные цены)"
    else:
        prices = synthetic_prices(rng)
        price_source = "условные цены (нет COINGECKO_API_KEY)"

    btc = prices["bitcoin"]

    def btc_return(d):
        prev, cur = btc.get(d - timedelta(days=1)), btc.get(d)
        return (cur / prev - 1) if prev and cur else 0.0

    # ---------- users.csv ----------
    users = []
    for i in range(1, N_USERS + 1):
        country = pick(rng, COUNTRIES)
        if country in PILOT:
            signup = PILOT_START - timedelta(days=rng.randint(0, 10)) + timedelta(days=rng.randint(0, 50))
        else:
            signup = START - timedelta(days=rng.randint(0, 700))
        users.append({
            "user_id": f"U{i:05d}",
            "country_code": country,
            "signup_date": signup.isoformat(),
            "plan": pick(rng, PLANS),
            "age_group": pick(rng, AGE_GROUPS),
            "acquisition_channel": pick(rng, CHANNELS),
        })
    with open(OUT / "users.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(users[0]))
        w.writeheader()
        w.writerows(users)

    # ---------- users_kyc_seed.csv (стартовое состояние «базы сайта») ----------
    kyc = {u["user_id"]: pick(rng, KYC) for u in users}
    with open(OUT / "users_kyc_seed.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "kyc_status", "updated_at"])
        for u in users:
            w.writerow([u["user_id"], kyc[u["user_id"]], f"{u['signup_date']}T12:00:00Z"])

    # ---------- trades/trades_YYYY-MM-DD.csv ----------
    (OUT / "trades").mkdir(exist_ok=True)
    verified = [u for u in users if kyc[u["user_id"]] == "verified"]
    coin_names = list(COINS)
    coin_weights = [v[0] for v in COINS.values()]
    fields = ["trade_id", "trade_ts", "user_id", "coin_id", "side", "amount_eur", "fee_eur", "status"]
    trade_no, total = 0, 0

    for d in days(START, END):
        r = btc_return(d)
        active_pool = [
            u for u in verified
            if u["signup_date"] <= d.isoformat() and (u["country_code"] not in PILOT or d >= PILOT_START)
        ]
        weekend = 0.85 if d.weekday() >= 5 else 1.0
        n = int(rng.gauss(320, 25) * (1 + 8 * abs(r)) * weekend * len(active_pool) / len(verified))
        buy_prob = 0.55 + (0.12 if r < -0.03 else 0) - (0.10 if r > 0.03 else 0)

        rows = []
        for _ in range(max(n, 0)):
            trade_no += 1
            u = rng.choice(active_pool)
            ts = datetime.combine(d, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
            amount = round(rng.lognormvariate(math.log(80), 0.9) * AMOUNT_MULT[u["plan"]], 2)
            fee = round(max(amount * FEE_RATE[u["plan"]], MIN_FEE), 2)
            rows.append({
                "trade_id": f"T{trade_no:08d}",
                "trade_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "user_id": u["user_id"],
                "coin_id": rng.choices(coin_names, weights=coin_weights)[0],
                "side": "buy" if rng.random() < buy_prob else "sell",
                "amount_eur": f"{amount:.2f}",
                "fee_eur": f"{fee:.2f}",
                "status": pick(rng, STATUSES),
            })

        # «грязь» для проверки слоя ods и тестов качества
        for row in rows:
            x = rng.random()
            if x < 0.005:
                row["trade_ts"] = datetime.strptime(row["trade_ts"], "%Y-%m-%dT%H:%M:%SZ").strftime("%d.%m.%Y %H:%M:%S")
            elif x < 0.007:
                row["amount_eur"] = ""
                row["fee_eur"] = ""
        dups = [dict(row) for row in rng.sample(rows, k=max(1, len(rows) // 300))]
        rows.extend(dups)
        rows.sort(key=lambda row: row["trade_id"])

        with open(OUT / "trades" / f"trades_{d.isoformat()}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        total += len(rows)

    print(f"Цены: {price_source}")
    print(f"users.csv: {len(users)} строк; users_kyc_seed.csv: {len(users)} строк")
    print(f"trades: {(END - START).days + 1} файлов, {total} строк")


if __name__ == "__main__":
    main()
