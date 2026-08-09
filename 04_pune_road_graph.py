"""
04_pune_road_graph.py

Loads the real Pune road network from OpenStreetMap via OSMnx, confirms
it's usable, and saves it to disk so the generator doesn't re-fetch from
OSM every run.

Uses an explicit bounding box (not a place name) -- same lesson learned
during the NYC work: a place-name query can resolve to an odd or overly
large polygon depending on how OSM has it tagged administratively. A bbox
has no such ambiguity.

bbox format for osmnx 2.x is (west, south, east, north) -- confirmed the
hard way during the NYC traffic-matching work, where passing coordinates
in the wrong order silently produced a 39,000x oversized query.

Run:
    docker compose exec app python 04_pune_road_graph.py
"""

import osmnx as ox
import networkx as nx

# Covers central Pune + the corridors already used elsewhere in this
# project (Hinjewadi, Kharadi, Swargate, Pimpri-Chinchwad).
BBOX = (73.7300, 18.4700, 73.9500, 18.6350)  # (west, south, east, north)

GRAPH_PATH = "docs/samples/pune_road_graph.graphml"


def main():
    import os
    os.makedirs("docs/samples", exist_ok=True)

    print(f"Loading Pune road graph for bbox (west, south, east, north): {BBOX}")
    G = ox.graph_from_bbox(BBOX, network_type="drive")

    print(f"\nNodes (intersections): {G.number_of_nodes()}")
    print(f"Edges (road segments): {G.number_of_edges()}")

    is_connected = nx.is_strongly_connected(G)
    print(f"Strongly connected: {is_connected}")
    if not is_connected:
        largest = max(nx.strongly_connected_components(G), key=len)
        print(f"  Largest connected component: {len(largest)} of {G.number_of_nodes()} nodes")

    print("\n--- Sample edge attributes ---")
    u, v, data = next(iter(G.edges(data=True)))
    for key, value in data.items():
        print(f"  {key}: {value}")

    ox.save_graphml(G, filepath=GRAPH_PATH)
    print(f"\nSaved graph -> {GRAPH_PATH}")
    print("This is what 03_fake_data_generator.py will load to generate")
    print("synthetic readings on REAL road segments instead of jittered points.")


if __name__ == "__main__":
    main()
