#!/usr/bin/env python3
"""Replay diet records through the same endpoint a phone uses.

The CSV has clock times but no calendar dates, so each record uses today's
Asia/Shanghai date together with its original meal time.
"""

import csv
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as clock_time
from zoneinfo import ZoneInfo

import requests

CSV_PATH = "/home/ubuntu/data/diet_foods_extracted.csv"
URL = "http://localhost:8080/api/meal/record"
WORKERS = 16
ZONE = ZoneInfo("Asia/Shanghai")
PROGRESS_EVERY = 2000


def records():
    today = datetime.now(ZONE).date()
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                portion = float(row["portion_g"])
            except ValueError:
                continue
            if portion != portion or portion <= 0:
                continue
            hour, minute, *_ = (int(part) for part in row["eating_time"].split(":"))
            eaten_at = datetime.combine(today, clock_time(hour, minute), ZONE)
            yield {
                "line": line_number,
                "payload": {
                    "userId": row["respondent_id"].strip(),
                    "timestamp": int(eaten_at.timestamp() * 1000),
                    "mealType": row["meal_type"].strip()[:32],
                    "foodCode": row["usda_food_code"].strip(),
                    "foodName": row["food_name"].strip()[:128],
                    "portionG": portion,
                },
            }


def main():
    jobs = list(records())
    total = len(jobs)
    print(f"valid_records={total}", flush=True)
    ok = 0
    failed = 0
    failures = []
    lock = threading.Lock()
    started = time.monotonic()
    thread_local = threading.local()

    def session():
        client = getattr(thread_local, "session", None)
        if client is None:
            client = requests.Session()
            thread_local.session = client
        return client

    def send(job):
        response = session().post(URL, json=job["payload"], timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
        return job["line"]

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(send, job) for job in jobs]
        for finished, future in enumerate(as_completed(futures), start=1):
            try:
                future.result()
                with lock:
                    ok += 1
            except Exception as error:
                with lock:
                    failed += 1
                    if len(failures) < 10:
                        failures.append(str(error))
            if finished % PROGRESS_EVERY == 0 or finished == total:
                elapsed = time.monotonic() - started
                print(
                    f"sent={finished} ok={ok} failed={failed} elapsed_s={elapsed:.1f}",
                    flush=True,
                )

    print(json.dumps({"ok": ok, "failed": failed, "sample_failures": failures}, ensure_ascii=False))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
