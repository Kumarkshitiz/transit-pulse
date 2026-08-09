"""
05_spark_streaming.py

Stage 4 of the pipeline: traffic-enriched -> Spark Structured Streaming.

Starts simple, on purpose -- same discipline as every earlier stage:
read the stream, parse it, print it to console, and CONFIRM it's parsing
correctly before building any aggregation on top of it. Once you can see
clean rows printing here, the natural next step is a windowed aggregation
(e.g. avg current_speed_kmh per segment_name over a 1-minute window) --
but that's a deliberate next step, not this one.

Schema mirrors the JSON records produced by 03_fake_data_generator.py.
All fields are nullable -- if an older record (e.g. from the earlier
batch/cycle-based version of the generator) is still sitting in the
topic and has a slightly different shape, this won't crash, it'll just
show nulls for whatever's missing.

Run:
    docker compose exec spark spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      05_spark_streaming.py

Stop:
    Ctrl+C in that terminal
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
SOURCE_TOPIC = "traffic-enriched"

SCHEMA = StructType([
    StructField("segment_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("length_m", LongType(), True),
    StructField("on_real_road_segment", BooleanType(), True),
    StructField("current_speed_kmh", DoubleType(), True),
    StructField("free_flow_speed_kmh", DoubleType(), True),
    StructField("current_travel_time_s", LongType(), True),
    StructField("free_flow_travel_time_s", LongType(), True),
    StructField("congestion_level", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("road_class", StringType(), True),
    StructField("road_closed", BooleanType(), True),
    StructField("based_on", StringType(), True),
    StructField("simulated", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("TransitPulseLite-Stage4-Console")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # Spark's default INFO logging is very noisy

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic '{SOURCE_TOPIC}'...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")  # only new messages, not full history
        .load()
    )

    # Kafka gives us raw key/value bytes -- cast value to string, then
    # parse the JSON string into actual typed columns using SCHEMA.
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")  # matches the generator's fire interval
        .start()
    )

    print("Streaming query started. Waiting for data... (Ctrl+C to stop)\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
