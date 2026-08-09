"""
03_fake_data_generator.py (continuous / real-time, timer-driven)

Stage 3 of the pipeline: traffic-raw -> generator -> traffic-enriched.

Runs on its OWN clock now, decoupled from how often real TomTom readings
arrive: every FIRE_INTERVAL_SECONDS (default 10s), it generates and sends
a fresh batch of SYNTHETIC_PER_SEGMENT synthetic segments for every
corridor it currently has a cached real reading for.

The cache is kept up to date by continuously polling 'traffic-raw' in the
background (non-blocking, short poll) -- whenever a new real reading
arrives (e.g. every 180s from 02_produce_to_kafka.py), it just replaces
that corridor's cached reading. The generator itself doesn't wait on
Kafka messages to fire; it fires on its own 10s timer regardless, reusing
the last real reading it has for each corridor until a fresher one shows up.

This means: real TomTom data still only updates every few minutes (rate
limited, see 02_produce_to_kafka.py), but synthetic output flows every
10 seconds using the current wall-clock time for the rush-hour model --
so congestion values shift smoothly between real refreshes instead of
jumping only when TomTom is polled.

Uses a Kafka consumer group ('fake-data-generator') with
auto_offset_reset='latest' so a restart doesn't replay old history.

Run (in a second terminal, alongside 02_produce_to_kafka.py running in
the first):
    docker compose exec app python 03_fake_data_generator.py
Stop:
    Ctrl+C in that terminal
"""

import json
import math
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SOURCE_TOPIC = "traffic-raw"
DEST_TOPIC = "traffic-enriched"
CONSUMER_GROUP = "fake-data-generator"

SYNTHETIC_PER_SEGMENT = 20   # spatial expansion: fake nearby segments per real reading
FIRE_INTERVAL_SECONDS = int(os.getenv("GENERATOR_FIRE_INTERVAL_SECONDS", "10"))

GRAPH_PATH = "docs/samples/pune_road_graph.graphml"

# Fallback jitter/length, only used if the real road graph isn't loaded.
COORD_JITTER = 0.003
SYNTHETIC_LENGTH_RANGE_M = (300, 1500)
EDGE_SEARCH_RADIUS_DEG = 0.012
ROAD_CLOSED_PROBABILITY = 0.002

IST = timezone(timedelta(hours=5, minutes=30))

_stop = False


def _handle_stop(signum, frame):
    global _stop
    print(f"\nReceived signal {signum} -- stopping after current batch...")
    _stop = True


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)


def load_road_graph():
    """Loads the real Pune road graph if 04_pune_road_graph.py has been run.
    Returns None if not found -- callers fall back to jittered fake points."""
    if not os.path.exists(GRAPH_PATH):
        print(f"No road graph found at {GRAPH_PATH}.")
        print("Run 04_pune_road_graph.py first for real road segments;")
        print("falling back to jittered fake points for now.\n")
        return None

    import osmnx as ox
    print(f"Loading real road graph from {GRAPH_PATH}...")
    G = ox.load_graphml(GRAPH_PATH)
    print(f"Loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.\n")
    return G


def find_nearby_edges(G, lat: float, lon: float, max_edges: int = 200) -> list:
    """Cheap bounding-box filter for real road graph edges near a point --
    intentionally returns MANY nearby edges, not just the single closest one."""
    nearby = []
    for u, v, data in G.edges(data=True):
        u_data, v_data = G.nodes[u], G.nodes[v]
        mid_lat = (float(u_data["y"]) + float(v_data["y"])) / 2
        mid_lon = (float(u_data["x"]) + float(v_data["x"])) / 2

        if abs(mid_lat - lat) <= EDGE_SEARCH_RADIUS_DEG and abs(mid_lon - lon) <= EDGE_SEARCH_RADIUS_DEG:
            nearby.append({
                "latitude": mid_lat,
                "longitude": mid_lon,
                "length_m": data.get("length", random.randint(*SYNTHETIC_LENGTH_RANGE_M)),
                "name": data.get("name", "unnamed"),
            })
            if len(nearby) >= max_edges:
                break

    return nearby


def congestion_factor_for_time(now_utc: datetime) -> float:
    """Returns a 0-1 multiplier on free-flow speed, modeling morning (~9:30)
    and evening (~19:30) rush hours in Pune (IST) as Gaussian dips."""
    now_ist = now_utc.astimezone(IST)
    hour = now_ist.hour + now_ist.minute / 60

    def rush_hour_dip(center_hour: float, width: float, depth: float) -> float:
        return depth * math.exp(-((hour - center_hour) ** 2) / (2 * width ** 2))

    dip = rush_hour_dip(9.5, 1.5, 0.55) + rush_hour_dip(19.5, 1.5, 0.50)
    base_factor = 1.0 - min(dip, 0.75)  # never drop below 25% of free-flow speed

    noise = random.uniform(-0.08, 0.08)
    return max(0.15, min(1.0, base_factor + noise))


def congestion_level_from_ratio(ratio: float) -> str:
    if ratio >= 0.85:
        return "free"
    elif ratio >= 0.5:
        return "moderate"
    return "heavy"


def generate_synthetic(real: dict, index: int, now: datetime, edge: dict = None) -> dict:
    free_flow = real.get("free_flow_speed_kmh") or real.get("current_speed_kmh") or 30

    factor = congestion_factor_for_time(now)
    current_speed = round(free_flow * factor, 1)
    current_speed = max(current_speed, 3.0)

    if edge is not None:
        latitude = round(edge["latitude"], 6)
        longitude = round(edge["longitude"], 6)
        length_m = round(float(edge["length_m"]))
        segment_label = edge["name"] if edge["name"] != "unnamed" else f"{real['segment_name']} - synthetic {index:02d}"
        on_real_road = True
    else:
        latitude = round(real["latitude"] + random.uniform(-COORD_JITTER, COORD_JITTER), 6)
        longitude = round(real["longitude"] + random.uniform(-COORD_JITTER, COORD_JITTER), 6)
        length_m = random.randint(*SYNTHETIC_LENGTH_RANGE_M)
        segment_label = f"{real['segment_name']} - synthetic {index:02d}"
        on_real_road = False

    length_km = length_m / 1000
    current_travel_time_s = round((length_km / current_speed) * 3600) if current_speed > 0 else None
    free_flow_travel_time_s = round((length_km / free_flow) * 3600) if free_flow > 0 else None

    return {
        "segment_name": segment_label,
        "latitude": latitude,
        "longitude": longitude,
        "length_m": length_m,
        "on_real_road_segment": on_real_road,
        "current_speed_kmh": current_speed,
        "free_flow_speed_kmh": free_flow,
        "current_travel_time_s": current_travel_time_s,
        "free_flow_travel_time_s": free_flow_travel_time_s,
        "congestion_level": congestion_level_from_ratio(factor),
        "confidence": round(random.uniform(0.7, 0.95), 2),
        "road_class": real["road_class"],
        "road_closed": random.random() < ROAD_CLOSED_PROBABILITY,
        "based_on": real["segment_name"],
        "simulated": True,
        "ingested_at": now.isoformat(),
    }


def refresh_cache_from_kafka(consumer, latest_real_by_corridor: dict, edges_by_corridor: dict, G) -> int:
    """Non-blocking poll for any new real readings -- updates the cache in
    place and returns how many messages were picked up. Never blocks longer
    than the consumer's configured timeout, so the 10s fire timer stays on schedule."""
    updated = 0
    for message in consumer:
        real = message.value
        latest_real_by_corridor[real["segment_name"]] = real
        updated += 1

        if real["segment_name"] not in edges_by_corridor and G is not None:
            nearby = find_nearby_edges(G, real["latitude"], real["longitude"])
            edges_by_corridor[real["segment_name"]] = nearby
            print(f"  {real['segment_name']}: {len(nearby)} real road edges found nearby")

    return updated


def fire_batch(producer, latest_real_by_corridor: dict, edges_by_corridor: dict) -> int:
    """Generates and sends one synthetic batch per cached corridor, using
    whatever real reading is currently cached (fresh or not)."""
    now = datetime.now(timezone.utc)
    sent = 0
    for segment_name, real in latest_real_by_corridor.items():
        available_edges = edges_by_corridor.get(segment_name, [])
        for i in range(SYNTHETIC_PER_SEGMENT):
            edge = available_edges[i % len(available_edges)] if available_edges else None
            record = generate_synthetic(real, i, now, edge=edge)
            producer.send(DEST_TOPIC, key=record["segment_name"], value=record)
            sent += 1

    producer.flush()
    return sent, now


def main():
    G = load_road_graph()
    edges_by_corridor = {}
    latest_real_by_corridor = {}  # corridor name -> most recent real reading seen

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=500,  # short, non-blocking-ish poll so the fire timer isn't delayed
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"Firing a synthetic batch every {FIRE_INTERVAL_SECONDS}s, "
          f"caching real readings from '{SOURCE_TOPIC}' in the background.")
    print("Waiting for the first real reading before the first batch can fire...\n")

    total_sent = 0
    last_fire = 0.0  # monotonic timestamp of the last fired batch
    try:
        while not _stop:
            refresh_cache_from_kafka(consumer, latest_real_by_corridor, edges_by_corridor, G)

            now_mono = time.monotonic()
            if latest_real_by_corridor and (now_mono - last_fire) >= FIRE_INTERVAL_SECONDS:
                sent, now = fire_batch(producer, latest_real_by_corridor, edges_by_corridor)
                total_sent += sent
                last_fire = now_mono
                print(f"{now.isoformat()}: fired {sent} synthetic messages "
                      f"across {len(latest_real_by_corridor)} cached corridor(s) "
                      f"(total sent: {total_sent})")
    finally:
        consumer.close()
        producer.close()
        print(f"\nStopped. {total_sent} total synthetic messages sent to '{DEST_TOPIC}'.")


if __name__ == "__main__":
    main()