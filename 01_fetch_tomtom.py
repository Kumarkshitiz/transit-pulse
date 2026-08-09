
"""
01_fetch_tomtom.py

Purpose: the FIRST and ONLY thing this script does is confirm your TomTom
key actually works for the Flow Segment Data endpoint. No Kafka, no
database, nothing else -- just one real HTTP request and the raw response,
so we know immediately if there's a billing/account issue rather than
finding out three layers deep in a pipeline.

Confirmed URL shape (verified against TomTom's live docs):
https://api.tomtom.com/traffic/services/4/flowSegmentData/{style}/{zoom}/{format}
    ?key=...&point={lat},{lon}&unit=kmph

Run:
    docker compose exec app python 01_fetch_tomtom.py
"""

import os

import requests
from dotenv import load_dotenv

load_dotenv()

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

# One real Pune point to test against -- Hinjewadi Phase 1, a known
# congestion corridor.
TEST_LAT, TEST_LON = 18.5913, 73.7389

URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def main():
    if not TOMTOM_API_KEY or TOMTOM_API_KEY == "paste_your_key_here":
        print("TOMTOM_API_KEY is not set in .env -- edit .env first.")
        return

    params = {
        "key": TOMTOM_API_KEY,
        "point": f"{TEST_LAT},{TEST_LON}",
        "unit": "kmph",
    }

    print(f"Requesting live traffic for Hinjewadi Phase 1 ({TEST_LAT}, {TEST_LON})...")
    resp = requests.get(URL, params=params, timeout=15)

    print(f"\nHTTP status: {resp.status_code}")

    if resp.status_code == 200:
        data = resp.json()["flowSegmentData"]
        print("\nSUCCESS -- key works, real live data below:")
        print(f"  Current speed:    {data.get('currentSpeed')} km/h")
        print(f"  Free-flow speed:  {data.get('freeFlowSpeed')} km/h")
        print(f"  Confidence:       {data.get('confidence')}")
        print(f"  Road class:       {data.get('frc')}")
    elif resp.status_code == 403:
        print("\n403 Forbidden -- this usually means the key isn't authorized")
        print("for this specific API/plan. Full response body below:")
        print(resp.text)
    elif resp.status_code == 401:
        print("\n401 Unauthorized -- the key itself is invalid or missing.")
        print(resp.text)
    else:
        print(f"\nUnexpected status. Full response body:")
        print(resp.text)


if __name__ == "__main__":
    main()
