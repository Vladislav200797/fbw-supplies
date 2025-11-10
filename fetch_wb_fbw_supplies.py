#!/usr/bin/env python3
import os, sys, time, math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import requests
from supabase import create_client, Client

# ====== ENV ======
WB_SUPPLIES_TOKEN     = os.getenv("WB_SUPPLIES_TOKEN")          # HeaderApiKey (Authorization)
SUPABASE_URL          = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")        # service_role
SCHEMA                = os.getenv("SUPABASE_SCHEMA", "public")
TABLE                 = os.getenv("SUPABASE_TABLE", "fbw_supplies")
SUPPLIES_DAYS         = int(os.getenv("SUPPLIES_DAYS", "365"))   # период выгрузки, дней назад (по умолчанию 365)
STATUSES_ENV          = os.getenv("SUPPLIES_STATUSES", "1,2,3,4,5,6")  # какие статусы тянуть

API_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
HEADERS = {"Authorization": WB_SUPPLIES_TOKEN, "Content-Type": "application/json"}

# ====== Helpers ======
def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def msk_now():
    return datetime.now(timezone(timedelta(hours=3)))  # MSK (UTC+3)

def msk_iso(dt: datetime) -> str:
    # Форматируем в ISO 8601 с +03:00
    return dt.isoformat(timespec="seconds")

def fetch_chunk(offset: int, limit: int, date_start: str, date_end: str, statuses: List[int]) -> List[Dict[str, Any]]:
    body = {
        "dates": [{"start": date_start, "end": date_end}],
        "statusIDs": statuses
    }
    params = {"limit": limit, "offset": offset}
    resp = requests.post(API_URL, headers=HEADERS, json=body, params=params, timeout=60)
    if resp.status_code != 200:
        fail(f"WB API {resp.status_code}: {resp.text}")
    data = resp.json()
    if not isinstance(data, list):
        fail(f"Unexpected WB response: {data}")
    return data

def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        supply_id  = r.get("supplyID")
        preorder_id = r.get("preorderID")
        if supply_id:
            wb_key = f"S:{int(supply_id)}"
        else:
            wb_key = f"P:{int(preorder_id) if preorder_id is not None else 0}"

        def as_ts(k):
            v = r.get(k)
            return v if v is not None else None  # строка ISO8601, PostgREST приведёт к timestamptz

        out.append({
            "wb_key": wb_key,
            "supply_id": supply_id,
            "preorder_id": preorder_id,
            "phone": r.get("phone"),
            "create_date": as_ts("createDate"),
            "supply_date": as_ts("supplyDate"),
            "fact_date": as_ts("factDate"),
            "updated_date": as_ts("updatedDate"),
            "status_id": r.get("statusID"),
        })
    return out

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")

    # Период выгрузки (MSK): последние SUPPLIES_DAYS
    end_dt   = msk_now()
    start_dt = end_dt - timedelta(days=SUPPLIES_DAYS)
    date_start = msk_iso(start_dt.replace(hour=0, minute=0, second=0, microsecond=0))
    date_end   = msk_iso(end_dt)

    statuses = [int(x) for x in STATUSES_ENV.split(",") if x.strip()]

    # Пагинация
    limit = 1000
    offset = 0
    all_rows: List[Dict[str, Any]] = []
    backoffs = [0, 2, 5]

    while True:
        for i, wait in enumerate(backoffs):
            if wait:
                time.sleep(wait)
            try:
                rows = fetch_chunk(offset, limit, date_start, date_end, statuses)
                break
            except SystemExit:
                raise
            except Exception as e:
                if i == len(backoffs)-1:
                    raise
                continue

        all_rows.extend(rows)
        if len(rows) < limit:
            break
        offset += limit

    print(f"Fetched {len(all_rows)} supplies from {date_start} to {date_end} (MSK)")

    # Нормализация и загрузка
    payload = normalize_rows(all_rows)
    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Полный refresh: очищаем и вставляем
    sb.schema(SCHEMA).table(TABLE).delete().neq("wb_key", "").execute()

    inserted = 0
    for batch in chunked(payload, 500):
        sb.schema(SCHEMA).table(TABLE).insert(batch).execute()
        inserted += len(batch)

    print(f"Inserted rows: {inserted}")

if __name__ == "__main__":
    main()
