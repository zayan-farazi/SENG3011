import os
import time
import uuid
import random
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from constants import  (LOCATION_PATH,STATUS_OK, RETRIEVE_RAW_WEATHER_PATH, INGEST_WEATHER_PATH,
                        RETRIEVE_PROCESSED_WEATHER_PATH, PROCESS_WEATHER_PATH, RISK_LOCATION_PATH, WATCHLIST_PATH, PATHFINDING_PATH )
from tests.test_constants import (
MAX_WORKERS,
CONCURRENT_WORKERS,
LOW_TIME_OUT,
MED_TIME_OUT,
HIGH_TIME_OUT, 
HUB_ID_1,
DATE_2, 
RAW_WEATHER_DATA_H1
)

BASE_URL = os.environ["STAGING_BASE_URL"]

TEST_HUB_ID = HUB_ID_1
TEST_HUB_ID_2 = "H002"
TEST_EMAIL = "test@example.com"

def _unique_location_payload():
    unique_suffix = uuid.uuid4().hex
    lat = random.uniform(-90, 90)
    lon = random.uniform(-180, 180)

    return {
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "name": f"StressLoc {unique_suffix[:10]}"
    }

# LOCATION
def create_location():
    url = f"{BASE_URL}/{LOCATION_PATH}"
    payload = _unique_location_payload()

    start = time.time()
    try:
        res = requests.post(url, json=payload, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

def list_locations():
    url = f"{BASE_URL}/{LOCATION_PATH}/list"
    start = time.time()
    try:
        res = requests.get(url, timeout=HIGH_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}
    
def get_location():
    url = f"{BASE_URL}/{LOCATION_PATH}/{TEST_HUB_ID}"
    start = time.time()
    try:
        res = requests.get(url, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}
# RETRIEVAL

def retrieve_raw():
    url = f"{BASE_URL}/{RETRIEVE_RAW_WEATHER_PATH}/{TEST_HUB_ID}"
    start = time.time()
    try:
        res = requests.get(url, params={"date": DATE_2}, timeout=LOW_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}
    
def retrieve_processed():
    url = f"{BASE_URL}/{RETRIEVE_PROCESSED_WEATHER_PATH}/{TEST_HUB_ID}"
    start = time.time()
    try:
        res = requests.get(url, params={"date": DATE_2}, timeout=LOW_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

# INGESTION

def ingest_weather():
    url = f"{BASE_URL}/{INGEST_WEATHER_PATH}/{TEST_HUB_ID}"
    start = time.time()
    try:
        res = requests.post(url, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}
    
# PROCESSING
def process_weather():
    url = f"{BASE_URL}/{PROCESS_WEATHER_PATH}"
    start = time.time()
    with open(RAW_WEATHER_DATA_H1, "r") as f:
        data = json.load(f)
    try:
        res = requests.post(url, json=data, timeout=HIGH_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

# ANALYTICS
def risk_location():
    url = f"{BASE_URL}/{RISK_LOCATION_PATH}/{TEST_HUB_ID}"
    start = time.time()
    try:
        res = requests.get(url,params={"date": DATE_2}, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

# WATCHLIST
def add_watchlist():
    url = f"{BASE_URL}/{WATCHLIST_PATH}/{TEST_HUB_ID}/{TEST_EMAIL}"
    start = time.time()
    try:
        res = requests.post(url, timeout=LOW_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

def remove_watchlist():
    url = f"{BASE_URL}/{WATCHLIST_PATH}/{TEST_HUB_ID}/{TEST_EMAIL}"
    start = time.time()
    try:
        res = requests.delete(url, timeout=LOW_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

def get_watchlist_hubs():
    url = f"{BASE_URL}/{WATCHLIST_PATH}/{TEST_EMAIL}"
    start = time.time()
    try:
        res = requests.get(url, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

def get_watchlist_messages():
    url = f"{BASE_URL}/{WATCHLIST_PATH}/messages/{TEST_EMAIL}"
    start = time.time()
    try:
        res = requests.get(url, timeout=MED_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}

# PATHFINDING

def optimal_path():
    url = f"{BASE_URL}/ese/v1/pathfinding/{TEST_HUB_ID}/{TEST_HUB_ID_2}"
    start = time.time()
    try:
        res = requests.get(url, timeout=HIGH_TIME_OUT)
        return {"success": res.status_code == STATUS_OK, "latency": time.time() - start}
    except:
        return {"success": False}


# STRESS WRAPPER

def run_stress(worker_fn, timeout, max_test_duration):
    users = MAX_WORKERS
    concurrency = CONCURRENT_WORKERS
    results = []

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker_fn) for _ in range(users)]

        try:
            for f in as_completed(futures, timeout=max_test_duration):
                results.append(f.result())
                if time.time() - start_time > max_test_duration:
                    break
        except Exception:
            pass

        for f in futures:
            f.cancel()

    total_time = time.time() - start_time

    completed = len(results)
    failures = sum(1 for r in results if not r["success"])
    latencies = [r["latency"] for r in results if r.get("latency") and r["success"]]

    avg_latency = sum(latencies) / len(latencies) if latencies else 0

    if total_time >= max_test_duration:
        raise AssertionError("Stress test exceeded max duration")

    # Failed if endpoint call failure exceeds 5% of the total endpoints
    assert failures < completed * 0.05

    # Failed if the average latency is more than half the timeout of the endpoint call
    assert avg_latency < timeout / 2
