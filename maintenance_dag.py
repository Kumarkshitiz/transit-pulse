"""
maintenance_dag.py

Runs weekly. Handles upkeep that doesn't belong on a 5-minute health-check
cadence:

  1. rebuild_road_graph -- re-fetches the Pune road network from OSM and
     overwrites docs/samples/pune_road_graph.graphml (same logic as
     04_pune_road_graph.py). Road networks do change, even if slowly --
     weekly is a reasonable cadence for a project this size.

     NOTE: 07_routing_api.py only loads the graph at startup. After this
     task runs, restart the 'api' service (docker compose restart api)
     for the refreshed graph to actually take effect -- this DAG doesn't
     do that automatically (would need Docker-socket access from inside
     the Airflow container, a deliberate scope cut for now).

  2. check_disk_usage -- logs how much space the project volume is using.
     Kafka expires old messages on its own (default 7-day retention), so
     this is an early-warning log, not active cleanup.
"""

import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"
GRAPH_PATH = f"{PROJECT_DIR}/docs/samples/pune_road_graph.graphml"
BBOX = (73.7300, 18.4700, 73.9500, 18.6350)  # must match 04_pune_road_graph.py
DISK_WARN_THRESHOLD_GB = 5


def rebuild_road_graph(**context):
    import osmnx as ox

    print(f"Re-fetching Pune road graph for bbox {BBOX}...")
    G = ox.graph_from_bbox(BBOX, network_type="drive")
    print(f"Fetched: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    ox.save_graphml(G, filepath=GRAPH_PATH)
    print(f"Saved -> {GRAPH_PATH}")
    print("Remember: `docker compose restart api` for this to take effect "
          "(it only loads the graph at startup).")


def check_disk_usage(**context):
    total, used, free = shutil.disk_usage(PROJECT_DIR)
    used_gb = used / (1024 ** 3)
    free_gb = free / (1024 ** 3)
    if free_gb < DISK_WARN_THRESHOLD_GB:
        print(f"WARN: only {free_gb:.1f}GB free on the project volume (used: {used_gb:.1f}GB).")
    else:
        print(f"OK: {free_gb:.1f}GB free on the project volume (used: {used_gb:.1f}GB).")


default_args = {"owner": "transitpulse-lite", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="pipeline_weekly_maintenance",
    description="Weekly road graph rebuild + disk usage check",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transitpulse", "maintenance"],
) as dag:
    PythonOperator(task_id="rebuild_road_graph", python_callable=rebuild_road_graph)
    PythonOperator(task_id="check_disk_usage", python_callable=check_disk_usage)
