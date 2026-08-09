"""
health_check_dag.py

Runs every 5 minutes. Does NOT run or replace any of the always-on
pipeline services (Kafka producer/generator, Spark edge-weights job) --
those are managed directly by docker-compose (the 'producer',
'generator', and 'edge-weights' services, each with
restart: unless-stopped) so `docker compose up -d` alone brings up the
whole pipeline.

This DAG is a periodic SAFETY NET around that always-on pipeline:
  1. check_kafka_activity        -- are traffic-raw / traffic-enriched
                                     still receiving new messages?
                                     (compares offsets to the previous
                                     run via an Airflow Variable)
  2. check_edge_weights_freshness -- is edge_weights.json still being
                                     updated by 06_edge_weights.py?
  3. check_tomtom_quota_budget    -- given the CURRENT corridor count
                                     and poll interval configured in
                                     02_produce_to_kafka.py, would
                                     today's request volume exceed
                                     TomTom's free-tier cap?

All checks currently just print a clear OK/WARN line to the task log --
wire up Airflow's EmailOperator or a Slack webhook later if you want an
actual notification instead of checking the Airflow UI.
"""

import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer, TopicPartition

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPICS = ["traffic-raw", "traffic-enriched"]
EDGE_WEIGHTS_PATH = "/opt/airflow/project/docs/samples/edge_weights.json"
EDGE_WEIGHTS_STALE_AFTER_SECONDS = 300  # should refresh every 30s -- 5 min stale is a real problem

TOMTOM_FREE_TIER_DAILY_CAP = 2500
# Mirrors BASE_SEGMENTS/discovered corridors + POLL_INTERVAL_SECONDS in
# 02_produce_to_kafka.py -- update these two numbers if you change either.
CURRENT_CORRIDOR_COUNT = 13
CURRENT_POLL_INTERVAL_SECONDS = 460


def check_kafka_activity(**context):
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, consumer_timeout_ms=5000)
    for topic in TOPICS:
        partitions = [TopicPartition(topic, p) for p in (consumer.partitions_for_topic(topic) or [0])]
        end_offsets = consumer.end_offsets(partitions)
        current_total = sum(end_offsets.values())

        var_key = f"kafka_offset_{topic}"
        previous_total = int(Variable.get(var_key, default_var=0))
        Variable.set(var_key, current_total)

        if current_total <= previous_total:
            print(f"WARN: '{topic}' offset did not increase since last check "
                  f"({previous_total} -> {current_total}). Producer/generator may be down.")
        else:
            print(f"OK: '{topic}' offset {previous_total} -> {current_total} "
                  f"(+{current_total - previous_total} messages)")
    consumer.close()


def check_edge_weights_freshness(**context):
    if not os.path.exists(EDGE_WEIGHTS_PATH):
        print(f"WARN: {EDGE_WEIGHTS_PATH} does not exist yet -- has 06_edge_weights.py run at least once?")
        return

    with open(EDGE_WEIGHTS_PATH) as f:
        data = json.load(f)

    updated_at = datetime.fromisoformat(data["updated_at"])
    age_seconds = (datetime.now(timezone.utc) - updated_at).total_seconds()

    if age_seconds > EDGE_WEIGHTS_STALE_AFTER_SECONDS:
        print(f"WARN: edge_weights.json is {age_seconds:.0f}s old (expected refresh every 30s). "
              f"Is the 'edge-weights' Spark service still running?")
    else:
        print(f"OK: edge_weights.json updated {age_seconds:.0f}s ago, "
              f"{len(data.get('segments', {}))} segments, citywide_ratio={data.get('citywide_ratio')}")


def check_tomtom_quota_budget(**context):
    polls_per_day = 86400 / CURRENT_POLL_INTERVAL_SECONDS
    requests_per_day = polls_per_day * CURRENT_CORRIDOR_COUNT
    if requests_per_day > TOMTOM_FREE_TIER_DAILY_CAP:
        print(f"WARN: configured for ~{requests_per_day:.0f} TomTom requests/day, "
              f"over the {TOMTOM_FREE_TIER_DAILY_CAP}/day free-tier cap.")
    else:
        margin = TOMTOM_FREE_TIER_DAILY_CAP - requests_per_day
        print(f"OK: ~{requests_per_day:.0f} TomTom requests/day, {margin:.0f}/day margin under the cap.")


default_args = {"owner": "transitpulse-lite", "retries": 0}

with DAG(
    dag_id="pipeline_health_check",
    description="Periodic safety net around the always-on Kafka/Spark pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transitpulse", "monitoring"],
) as dag:
    PythonOperator(task_id="check_kafka_activity", python_callable=check_kafka_activity)
    PythonOperator(task_id="check_edge_weights_freshness", python_callable=check_edge_weights_freshness)
    PythonOperator(task_id="check_tomtom_quota_budget", python_callable=check_tomtom_quota_budget)
