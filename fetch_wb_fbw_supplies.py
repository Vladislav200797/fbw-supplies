#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Выгрузка FBW поставок из WB в Supabase (schema public).
POST /api/v1/supplies требует dates[].Type как СТРОКУ:
'createDate' | 'supplyDate' | 'factDate' | 'updatedDate'.
Обходим все указанные типы дат, объединяем без дублей по wb_key,
полностью обновляем таблицу (delete -> insert).
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

import requests
from supabase import create_client, Client

# ===== Настройки из окружения =====
WB_SUPPLIES_TOKEN     = os.getenv("WB_SUPPLIES_TOKEN")            # HeaderApiKey (Authorization)
SUPABASE_URL          = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")          # service_role
SCHEMA                = os.getenv("SUPABASE_SCHEMA", "public")
TABLE                 = os.getenv("SUPABASE_TABLE", "fbw_supplies")

# Период выгрузки (дней назад от "сейчас по МСК")
SUPPLIES_DAYS         = int(os.getenv("SUPPLIES_DAYS", "365"))

# Статусы поставок (1..6), по умолчанию все
STATUSES_ENV          = os.getenv("SUPPLIES_STATUSES", "1,2,3,4,5,6")
STATUSES              = [int(x) for x in STATUSES_ENV.split(",") if x.strip()]

# Типы дат для фильтра WB. ДОЛЖНЫ БЫТЬ строками: createDate,supplyDate,factDate,updatedDate
DATE_TYPES_ENV        = os.getenv("SUPPLIES_DATE_TYPES", "createDate,supplyDate,factDate,updatedDate")
DATE_TYPES            = [x.strip() for x in DATE_TYPES_ENV.split(",") if x.strip()]

API_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
HEADERS = {
    "Authorization": WB_SUPPLIES_TOKEN or "",
    "Content-Type": "application/json",
}

# ===== Вспомогательные =====
def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def msk_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))  # MSK (UTC+3)

def msk_iso(dt: datetime) -> str:
    # ISO 8601 с явной зоной +03:00
    return dt.isoformat(timespec="seconds")

def fetch_chunk(offset: int, limit: int, date_start: str, date_end: str, statuses: List[int], date_type: str) -> List[Dict[str, Any]]:
    """
    Один запрос в WB /supplies для конкретного типа даты (строка).
    """
    body = {
        "dates": [{"start": date_start, "end": date_end, "Type": date_type}],
        "statusIDs": statuses
    }
    params = {"limit": limit, "offset": offset}

    backoffs = [0, 2, 5]
    for attempt, wait in enumerate(backoffs, start=1):
        if wait:
            time.sleep(wait)
        resp = requests.post(API_URL, headers=HEADERS, json=body, params=params, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, list):
                fail(f"Unexpected WB response: {data}")
            return data
        if resp.status_code == 429 and attempt < len(backoffs):
            continue
        fail(f"WB API {resp.status_code}: {resp.text}")
    return []

def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Приводим ключи WB к колонкам нашей таблицы.
    Даты оставляем ISO-строками — PG сам приведёт к timestamptz.
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        supply_id   = r.get("supplyID")
        preorder_id = r.get("preorderID")

        if supply_id is not None:
            try:
                wb_key = f"S:{int(supply_id)}"
            except Exception:
                wb_key = f"S:{supply_id}"
        else:
            pid = 0 if preorder_id is None else preorder_id
            try:
                wb_key = f"P:{int(pid)}"
            except Exception:
                wb_key = f"P:{pid}"

        def ts(name: str):
            v = r.get(name)
            return v if v is not None else None

        out.append({
            "wb_key": wb_key,
            "supply_id": supply_id,
            "preorder_id": preorder_id,
            "phone": r.get("phone"),
            "create_date": ts("createDate"),
            "supply_date": ts("supplyDate"),
            "fact_date": ts("factDate"),
            "updated_date": ts("updatedDate"),
            "status_id": r.get("statusID"),
        })
    return out

def chunked(seq: List[Dict[str, Any]], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# ===== Основной сценарий =====
def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")
    if not DATE_TYPES:
        fail("SUPPLIES_DATE_TYPES is empty or invalid (expect comma list like 'createDate,updatedDate')")

    # Период по МСК
    end_dt   = msk_now()
    start_dt = end_dt - timedelta(days=SUPPLIES_DAYS)
    date_start = msk_iso(start_dt.replace(hour=0, minute=0, second=0, microsecond=0))
    date_end   = msk_iso(end_dt)

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    limit = 1000
    combined: Dict[str, Dict[str, Any]] = {}
    total_requests = 0

    for dt in DATE_TYPES:
        offset = 0
        while True:
            rows = fetch_chunk(offset, limit, date_start, date_end, STATUSES, dt)
            total_requests += 1
            for item in normalize_rows(rows):
                combined[item["wb_key"]] = item  # merge без дублей
            if len(rows) < limit:
                break
            offset += limit

    print(f"Fetched unique supplies: {len(combined)}; period: {date_start} → {date_end} (MSK); requests: {total_requests}")

    # Полный refresh
    sb.schema(SCHEMA).table(TABLE).delete().neq("wb_key", "").execute()

    payload = list(combined.values())
    inserted = 0
    for batch in chunked(payload, 500):
        sb.schema(SCHEMA).table(TABLE).insert(batch).execute()
        inserted += len(batch)

    print(f"Inserted rows: {inserted}")
    print("Sync completed")

if __name__ == "__main__":
    main()
