"""
07_routing_api.py

Stage 6: the actual user-facing service. Loads the real Pune road graph
once, keeps it in memory, refreshes edge weights from edge_weights.json
(written by 06_edge_weights.py) on a background timer, and exposes:

    POST /login     {"username": "..."}          -> {"token": "..."}
    GET  /geocode    ?q=<address text>            -> [{"label","lat","lng"}, ...]
    POST /route      {"origin": {...}, "destination": {...}}
                                                   -> {"path": [[lat,lng], ...],
                                                       "eta_seconds": ..., "distance_m": ...}

NOTE ON AUTH: /login is intentionally trivial (username only, no
password, no real session security) -- this is built for a ~5-person
test, not production. Do not reuse this as-is for real user accounts.

NOTE ON GEOCODING: uses OpenStreetMap's free Nominatim service, which
has a strict usage policy (max ~1 request/second, requires a real
User-Agent). Fine for 5 testers clicking around; would need a paid
geocoder (or self-hosted Nominatim) before any real traffic.

Run:
    docker compose exec app python 07_routing_api.py
(serves on 0.0.0.0:8000; frontend/index.html is served at "/")
"""

import json
import math
import os
import threading
import time
from datetime import datetime, timezone

import networkx as nx
import osmnx as ox
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

GRAPH_PATH = "docs/samples/pune_road_graph.graphml"
WEIGHTS_PATH = "docs/samples/edge_weights.json"
WEIGHTS_REFRESH_SECONDS = 20
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "transitpulse-lite-test/1.0 (student project, contact: set-your-email-here)"

# Bounding box used for 04_pune_road_graph.py -- also used to bias geocoding
# results toward Pune instead of matching a same-named place elsewhere.
BBOX_WEST, BBOX_SOUTH, BBOX_EAST, BBOX_NORTH = 73.7300, 18.4700, 73.9500, 18.6350

# Fallback speeds (km/h) by OSM highway tag, used when an edge has no
# maxspeed AND no live weight override -- see the road-class coverage
# discussion earlier in this project.
DEFAULT_SPEED_KMH_BY_HIGHWAY = {
    "motorway": 80, "trunk": 60, "primary": 50,
    "secondary": 40, "tertiary": 30, "residential": 20,
    "unclassified": 25, "living_street": 15, "service": 15,
}
FALLBACK_SPEED_KMH = 25  # for any highway tag not in the table above
MAX_PLAUSIBLE_SPEED_KMH = 80  # used for the A* heuristic -- must never underestimate travel time

_graph_lock = threading.Lock()
G = None
_known_users = set()


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _edge_name(data: dict):
    name = data.get("name")
    if isinstance(name, list):
        name = name[0] if name else None
    return name


def _edge_highway(data: dict):
    hwy = data.get("highway")
    if isinstance(hwy, list):
        hwy = hwy[0] if hwy else None
    return hwy


def load_graph_with_default_weights() -> nx.MultiDiGraph:
    print(f"Loading road graph from {GRAPH_PATH}...")
    graph = ox.load_graphml(GRAPH_PATH)
    print(f"Loaded: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges. "
          f"Computing default (free-flow) weights...")

    for u, v, k, data in graph.edges(keys=True, data=True):
        length_m = float(data.get("length", 50))
        hwy = _edge_highway(data)
        speed_kmh = DEFAULT_SPEED_KMH_BY_HIGHWAY.get(hwy, FALLBACK_SPEED_KMH)
        default_time_s = (length_m / 1000) / speed_kmh * 3600
        graph[u][v][k]["default_time_s"] = default_time_s
        graph[u][v][k]["time_s"] = default_time_s  # live weight, refreshed periodically

    print("Default weights computed.\n")
    return graph


def refresh_weights_from_file():
    """Reads edge_weights.json (if present) and updates each edge's live
    'time_s': direct override for edges with a matching segment_name,
    citywide_ratio-based estimate for everything else. Never crashes the
    server if the file is missing or mid-write -- just skips this refresh."""
    global G
    if not os.path.exists(WEIGHTS_PATH):
        return

    try:
        with open(WEIGHTS_PATH, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return  # caught it mid-write or similar transient issue -- try again next cycle

    segments = data.get("segments", {})
    citywide_ratio = data.get("citywide_ratio", 1.0)
    citywide_ratio = max(citywide_ratio, 0.1)  # guard against runaway travel-time blowup

    updated_direct = 0
    with _graph_lock:
        for u, v, k, edge_data in G.edges(keys=True, data=True):
            name = _edge_name(edge_data)
            default_time_s = edge_data.get("default_time_s", 60)

            if name and name in segments:
                seg = segments[name]
                current_speed = max(seg.get("current_speed_kmh", 0), 1.0)
                length_m = float(edge_data.get("length", 50))
                G[u][v][k]["time_s"] = (length_m / 1000) / current_speed * 3600
                updated_direct += 1
            else:
                G[u][v][k]["time_s"] = default_time_s / citywide_ratio

    print(f"[{datetime.now(timezone.utc).isoformat()}] weights refreshed: "
          f"{updated_direct} edges direct-matched, citywide_ratio={citywide_ratio}")


def weight_refresh_loop():
    while True:
        try:
            refresh_weights_from_file()
        except Exception as e:
            print(f"weight refresh error (continuing): {e}")
        time.sleep(WEIGHTS_REFRESH_SECONDS)


def astar_heuristic(u, v):
    lat1, lon1 = G.nodes[u]["y"], G.nodes[u]["x"]
    lat2, lon2 = G.nodes[v]["y"], G.nodes[v]["x"]
    dist_m = haversine_m(float(lat1), float(lon1), float(lat2), float(lon2))
    return dist_m / (MAX_PLAUSIBLE_SPEED_KMH * 1000 / 3600)


app = FastAPI(title="TransitPulse-lite Routing API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # fine for a 5-person test; tighten before wider use
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    global G
    G = load_graph_with_default_weights()
    t = threading.Thread(target=weight_refresh_loop, daemon=True)
    t.start()


class LoginRequest(BaseModel):
    username: str


class LatLng(BaseModel):
    lat: float
    lng: float


class RouteRequest(BaseModel):
    origin: LatLng
    destination: LatLng


@app.post("/login")
def login(req: LoginRequest):
    username = req.username.strip()
    if not username:
        raise HTTPException(400, "username cannot be empty")
    _known_users.add(username)
    # Trivial "token" -- see the auth note in the module docstring.
    return {"token": username, "username": username}


@app.get("/geocode")
def geocode(q: str):
    if not q or len(q.strip()) < 2:
        raise HTTPException(400, "query too short")

    params = {
        "q": q,
        "format": "json",
        "limit": 5,
        "viewbox": f"{BBOX_WEST},{BBOX_NORTH},{BBOX_EAST},{BBOX_SOUTH}",
        "bounded": 1,
    }
    headers = {"User-Agent": NOMINATIM_USER_AGENT}
    resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
    resp.raise_for_status()

    results = [
        {"label": item["display_name"], "lat": float(item["lat"]), "lng": float(item["lon"])}
        for item in resp.json()
    ]
    return results


@app.post("/route")
def route(req: RouteRequest):
    with _graph_lock:
        try:
            orig_node = ox.distance.nearest_nodes(G, X=req.origin.lng, Y=req.origin.lat)
            dest_node = ox.distance.nearest_nodes(G, X=req.destination.lng, Y=req.destination.lat)
        except Exception as e:
            raise HTTPException(400, f"could not snap origin/destination to the road graph: {e}")

        try:
            node_path = nx.astar_path(G, orig_node, dest_node, heuristic=astar_heuristic, weight="time_s")
        except nx.NetworkXNoPath:
            raise HTTPException(404, "no route found between these points")

        coords = [[float(G.nodes[n]["y"]), float(G.nodes[n]["x"])] for n in node_path]

        eta_seconds = 0.0
        distance_m = 0.0
        for a, b in zip(node_path[:-1], node_path[1:]):
            # A node pair can have multiple parallel edges (MultiDiGraph) --
            # take the fastest one, matching what astar_path itself does.
            edge_options = G.get_edge_data(a, b)
            best = min(edge_options.values(), key=lambda d: d.get("time_s", float("inf")))
            eta_seconds += best.get("time_s", 0)
            distance_m += float(best.get("length", 0))

    return {
        "path": coords,
        "eta_seconds": round(eta_seconds),
        "distance_m": round(distance_m),
    }


# Serves frontend/index.html at "/" and any other static assets in that
# folder. Registered LAST so it doesn't shadow the API routes above.
if os.path.isdir("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
