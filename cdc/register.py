"""
Регистрирует (или обновляет) коннекторы CDC в Kafka Connect.

  users_kyc-source-orozova  — Debezium читает изменения orozova.users_kyc из WAL Postgres
  sink-orozova              — Debezium JDBC Sink делает upsert в orozova_ods.ods_users_kyc

В JSON-файлах вместо пароля стоит ${PG_PASSWORD} (НФТ-2). Пароль вводится при запуске
(не отображается на экране) или берётся из переменной окружения PG_PASSWORD
и подставляется только в запрос к Kafka Connect.

Запуск из папки project:
    python cdc/register.py            # зарегистрировать / обновить и показать статус
    python cdc/register.py status     # только статус
"""
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CONNECT = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
HERE = Path(__file__).resolve().parent
CONNECTORS = ["users_kyc-source-orozova", "sink-orozova"]


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONNECT + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def status():
    for name in CONNECTORS:
        code, st = call("GET", f"/connectors/{name}/status")
        if code != 200:
            print(f"{name}: не найден ({code})")
            continue
        tasks = ", ".join(f"task {t['id']}={t['state']}" for t in st.get("tasks", []))
        print(f"{name}: connector={st['connector']['state']}; {tasks or 'задач пока нет'}")
        for t in st.get("tasks", []):
            if t["state"] == "FAILED":
                print("   ошибка:", t.get("trace", "")[:600])


def register():
    password = os.environ.get("PG_PASSWORD") or getpass.getpass("Пароль от базы (не отображается): ")
    for name in CONNECTORS:
        text = (HERE / f"{name}.json").read_text(encoding="utf-8").replace("${PG_PASSWORD}", password)
        code, resp = call("PUT", f"/connectors/{name}/config", json.loads(text))
        print(f"{name}: {'OK' if code in (200, 201) else 'ошибка'} ({code})")
        if code not in (200, 201):
            print("  ", str(resp)[:600])
    time.sleep(5)
    status()


if __name__ == "__main__":
    status() if sys.argv[1:] == ["status"] else register()
