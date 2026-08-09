"""
06_edge_weights.py

Stage 5 of the pipeline: traffic-enriched -> Spark windowed aggregation
-> edge_weights.json (a shared file the routing API reads periodically).

Every TRIGGER_INTERVAL, aggregates the last WINDOW_DURATION of readings
per real segment_name into an average current/free-flow speed. Also
computes ONE citywide congestion ratio (current/free-flow, weighted by
reading count) as a fallback for the ~95% of the road graph with no
direct corridor coverage.

Known simplification: this uses a single citywide ratio rather than a
per-road-class ratio (which would need reconciling TomTom's FRC
classification against OSM's `highway` tags -- a real mapping, not a
1:1 match). Good enough to get routing working end to end; worth
revisiting once the basic app is working for your 5 testers.

Output is written atomically (temp file + os.replace) so the routing
API never reads a half-written file mid-update.

Run (alongside 02, 03, and optionally 05):
    docker compose exec spark /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      06_edge_weights.py
Stop:
    Ctrl+C in that terminal
"""

import json
import os
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
SOURCE_TOPIC = "traffic-enriched"
OUTPUT_PATH = "docs/samples/edge_weights.json"
WINDOW_DURATION = "2 minutes"
TRIGGER_INTERVAL = "30 seconds"

SCHEMA = StructType([
    StructField("segment_name", StringType(), True),
    StructField("current_speed_kmh", DoubleType(), True),
    StructField("free_flow_speed_kmh", DoubleType(), True),
    StructField("on_real_road_segment", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
])


def write_json_atomic(path: str, data: dict) -> None:
    dir_name = os.path.dirname(path) or "."
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)  # atomic on POSIX -- readers never see a partial file


def make_batch_writer():
    def write_batch(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            print(f"[batch {batch_id}] no rows this trigger, skipping write.")
            return

        segments = {}
        total_current_weighted = 0.0
        total_free_weighted = 0.0

        for row in rows:
            n = row["reading_count"]
            avg_current = row["avg_current_speed"]
            avg_free = row["avg_free_flow_speed"]
            if avg_current is None or avg_free is None or avg_free <= 0 or n <= 0:
                continue

            ratio = avg_current / avg_free
            # If a segment appears in multiple windows within this batch,
            # the later one (larger window end) simply overwrites -- fine
            # for this use case, we only care about the most recent state.
            segments[row["segment_name"]] = {
                "current_speed_kmh": round(avg_current, 2),
                "free_flow_speed_kmh": round(avg_free, 2),
                "ratio": round(ratio, 3),
                "reading_count": n,
            }
            total_current_weighted += avg_current * n
            total_free_weighted += avg_free * n

        citywide_ratio = (
            round(total_current_weighted / total_free_weighted, 3)
            if total_free_weighted > 0 else 1.0
        )

        output = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "batch_id": batch_id,
            "citywide_ratio": citywide_ratio,
            "segments": segments,
        }
        write_json_atomic(OUTPUT_PATH, output)
        print(f"[batch {batch_id}] wrote {len(segments)} segment weights, "
              f"citywide_ratio={citywide_ratio} -> {OUTPUT_PATH}")

    return write_batch


def main():
    spark = (
        SparkSession.builder
        .appName("TransitPulseLite-EdgeWeights")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("data"))
        .select("data.*")
        # Only real OSM edges carry a segment_name worth keying weights on --
        # jittered fallback records (on_real_road_segment=false) are skipped.
        .filter(col("on_real_road_segment") == True)
        .withColumn("event_time", col("ingested_at").cast("timestamp"))
    )

    windowed = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), WINDOW_DURATION), col("segment_name"))
        .agg(
            avg("current_speed_kmh").alias("avg_current_speed"),
            avg("free_flow_speed_kmh").alias("avg_free_flow_speed"),
            count("*").alias("reading_count"),
        )
    )

    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(make_batch_writer())
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"Aggregating '{SOURCE_TOPIC}' into edge weights every {TRIGGER_INTERVAL} "
          f"(window={WINDOW_DURATION}). Writing to {OUTPUT_PATH}.\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
