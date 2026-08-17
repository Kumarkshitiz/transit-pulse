"""
02_produce_to_kafka.py (continuous / real-time, auto-discovers corridors)

Stage 2 of the pipeline, real-time version: TomTom API -> Kafka, running
forever, now with automatic road-class coverage on top of the original
4 hand-picked arterials.

WHY: the 4 original corridors (Hinjewadi, Kharadi, Pimpri-Chinchwad,
Swargate) are all trunk/primary roads -- they give zero signal for
secondary/tertiary/residential streets, which is most of the actual
network a router would path through. At startup, this script loads the
road graph from 04_pune_road_graph.py and picks ONE representative real
edge per missing road class (secondary, tertiary, residential) to add
to the watch list -- deterministically (longest real-named edge per
class), not randomly, so the same corridors get picked every run and
readings stay comparable over time.

If the road graph isn't available yet, it just falls back to the
original 4 arterials -- this is an enhancement, not a hard requirement.

Rate-limit note: TomTom's free tier caps you around 2,500 requests/day.
Requests/day = (86400 / POLL_INTERVAL_SECONDS) * number_of_corridors.
This script computes that at startup and WARNS (but does not block) if
your current interval/corridor-count combination would exceed the cap --
see the printed budget check when you run it.

Run (in its own terminal, left running):
    docker compose exec app python 02_produce_to_kafka.py
Stop:
    Ctrl+C in that terminal
"""

import json
import os
import signal
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "traffic-raw"
POLL_INTERVAL_SECONDS = int(os.getenv("TOMTOM_POLL_INTERVAL_SECONDS", "460"))

TOMTOM_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

GRAPH_PATH = "docs/samples/pune_road_graph.graphml"

BASE_SEGMENTS = [
    {"name": "Hinjewadi Phase 1", "lat": 18.5913, "lng": 73.7389},
    {"name": "Kharadi - EON IT Park", "lat": 18.5510, "lng": 73.9430},
    {"name": "Pune-Mumbai Rd (Pimpri-Chinchwad)", "lat": 18.6298, "lng": 73.7997},
    {"name": "Swargate Junction", "lat": 18.5010, "lng": 73.8577},
    {"name": "Nal Stop", "lat": 18.5081, "lng": 73.8313},
    {"name": "Wakad Chowk", "lat": 18.5979, "lng": 73.7637},
    {"name": "Baner Road", "lat": 18.5581, "lng": 73.7927},
    {"name": "Viman Nagar", "lat": 18.5679, "lng": 73.9144},
    {"name": "Shivajinagar", "lat": 18.5303, "lng": 73.8499},
    {"name": "Warje", "lat": 18.4865, "lng": 73.7968},
]

TARGET_CLASSES = ["secondary", "tertiary", "residential"]

TOMTOM_FREE_TIER_DAILY_CAP = 2500

_stop = False


def _handle_stop(signum, frame):
    global _stop
    print(f"\nReceived signal {signum} -- finishing current poll, then stopping...")
    _stop = True


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)


def discover_additional_corridors() -> list:
    """
    Loads the real Pune road graph (if available) and picks ONE real edge
    per road class in TARGET_CLASSES to round out road-class coverage.

    Deterministic on purpose: picks the LONGEST real-named edge of each
    class (skipping "unnamed" edges) so the same corridor is chosen every
    run -- readings stay comparable day to day instead of jumping to a
    different random street each restart.

    Returns an empty list (not an error) if the graph file doesn't exist
    yet -- this is a coverage enhancement, not a hard dependency.
    """
    if not os.path.exists(GRAPH_PATH):
        print(f"No road graph found at {GRAPH_PATH} -- skipping auto-discovery,")
        print("using only the base arterial corridors. Run 04_pune_road_graph.py")
        print("first if you want automatic secondary/tertiary/residential coverage.\n")
        return []

    import osmnx as ox
    print(f"Loading road graph from {GRAPH_PATH} to discover additional corridors...")
    G = ox.load_graphml(GRAPH_PATH)

    best_by_class = {}  
    for u, v, data in G.edges(data=True):
        hwy = data.get("highway")
        hwy = hwy[0] if isinstance(hwy, list) else hwy
        if hwy not in TARGET_CLASSES:
            continue

        name = data.get("name")
        name = name[0] if isinstance(name, list) else name
        if not name:
            continue  

        length_m = data.get("length", 0)
        current_best = best_by_class.get(hwy)
        if current_best is None or length_m > current_best["length_m"]:
            mid_lat = (float(G.nodes[u]["y"]) + float(G.nodes[v]["y"])) / 2
            mid_lon = (float(G.nodes[u]["x"]) + float(G.nodes[v]["x"])) / 2
            best_by_class[hwy] = {
                "name": name,
                "lat": mid_lat,
                "lng": mid_lon,
                "length_m": length_m,
            }

    discovered = []
    for hwy in TARGET_CLASSES:
        pick = best_by_class.get(hwy)
        if pick:
            print(f"  {hwy}: found '{pick['name']}' ({pick['length_m']:.0f}m)")
            discovered.append({"name": f"{pick['name']} ({hwy})", "lat": pick["lat"], "lng": pick["lng"]})
        else:
            print(f"  {hwy}: no named edge found in this bbox -- no coverage added for this class")

    print()
    return discovered


CORRIDOR_STATE_PATH = "docs/samples/corridor_state.json"


def write_corridor_state(num_corridors: int, interval_seconds: int) -> None:
    """
    Persists the ACTUAL watched-corridor count and poll interval this run
    settled on (base arterials + whatever auto-discovery found), so other
    components -- specifically health_check_dag.py's TomTom quota check --
    can read the real number instead of relying on a hand-maintained
    constant that silently drifts whenever discovery finds a different
    number of corridors or POLL_INTERVAL_SECONDS changes.

    Written once at startup (the corridor list doesn't change mid-run).
    Atomic write, same pattern as 06_edge_weights.py's output, so a
    reader never sees a half-written file.
    """
    dir_name = os.path.dirname(CORRIDOR_STATE_PATH) or "."
    os.makedirs(dir_name, exist_ok=True)
    state = {
        "corridor_count": num_corridors,
        "poll_interval_seconds": interval_seconds,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp_path = CORRIDOR_STATE_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_path, CORRIDOR_STATE_PATH)


def check_rate_budget(num_corridors: int, interval_seconds: int) -> None:
    polls_per_day = 86400 / interval_seconds
    requests_per_day = polls_per_day * num_corridors
    print(f"Rate budget check: {num_corridors} corridors x {polls_per_day:.0f} polls/day "
          f"= {requests_per_day:.0f} requests/day (free-tier cap: {TOMTOM_FREE_TIER_DAILY_CAP}/day)")
    if requests_per_day > TOMTOM_FREE_TIER_DAILY_CAP:
        min_interval = 86400 * num_corridors / TOMTOM_FREE_TIER_DAILY_CAP
        print(f"  WARNING: this exceeds the free-tier cap. Raise "
              f"TOMTOM_POLL_INTERVAL_SECONDS to at least {min_interval:.0f}s, "
              f"or reduce the number of watched corridors.")
    print()


def fetch_tomtom(lat: float, lng: float) -> dict:
    params = {"key": TOMTOM_API_KEY, "point": f"{lat},{lng}", "unit": "kmph"}
    resp = requests.get(TOMTOM_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()["flowSegmentData"]


def poll_once(producer, segments, poll_number: int) -> int:
    sent = 0
    for segment in segments:
        try:
            raw = fetch_tomtom(segment["lat"], segment["lng"])
        except requests.RequestException as e:
            print(f"  [{poll_number}] FAILED to fetch {segment['name']}: {e}")
            continue

        record = {
            "segment_name": segment["name"],
            "latitude": segment["lat"],
            "longitude": segment["lng"],
            "current_speed_kmh": raw.get("currentSpeed"),
            "free_flow_speed_kmh": raw.get("freeFlowSpeed"),
            "confidence": raw.get("confidence"),
            "road_class": raw.get("frc"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(TOPIC, key=segment["name"], value=record)
        print(f"  [{poll_number}] {segment['name']} -> {record['current_speed_kmh']} km/h")
        sent += 1

    producer.flush()
    return sent


def main():
    if not TOMTOM_API_KEY or TOMTOM_API_KEY == "paste_your_key_here":
        print("TOMTOM_API_KEY is not set in .env -- edit .env first.")
        return

    additional = discover_additional_corridors()
    watched_segments = BASE_SEGMENTS + additional
    print(f"Watching {len(watched_segments)} corridors total "
          f"({len(BASE_SEGMENTS)} base arterials + {len(additional)} auto-discovered):")
    for s in watched_segments:
        print(f"  - {s['name']}")
    print()

    check_rate_budget(len(watched_segments), POLL_INTERVAL_SECONDS)
    write_corridor_state(len(watched_segments), POLL_INTERVAL_SECONDS)
    print(f"Wrote corridor state ({len(watched_segments)} corridors, "
          f"{POLL_INTERVAL_SECONDS}s interval) -> {CORRIDOR_STATE_PATH}\n")

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"Connected. Polling every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.\n")

    poll_number = 0
    total_sent = 0
    try:
        while not _stop:
            poll_number += 1
            now_str = datetime.now(timezone.utc).isoformat()
            print(f"Poll {poll_number} at {now_str}:")
            total_sent += poll_once(producer, watched_segments, poll_number)

            # Sleep in 1s increments so a stop signal during the wait is
            # picked up quickly instead of blocking for the full interval.
            slept = 0
            while slept < POLL_INTERVAL_SECONDS and not _stop:
                time.sleep(min(1, POLL_INTERVAL_SECONDS - slept))
                slept += 1
    finally:
        producer.close()
        print(f"\nStopped after {poll_number} polls, {total_sent} messages sent to '{TOPIC}'.")


if __name__ == "__main__":
    main()
