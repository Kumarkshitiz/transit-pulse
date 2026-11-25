# TransitPulse

**Real-time traffic ingestion, streaming analytics, and dynamic route planning for Pune.**

TransitPulse is a containerised data engineering project that combines live traffic observations from the TomTom Traffic Flow API with synthetic road-segment enrichment. Kafka transports the events, Spark Structured Streaming calculates rolling congestion metrics, and a FastAPI routing service applies the resulting traffic weights to a Pune road network built from OpenStreetMap data.

> **Project status:** The repository contains the implementation for the pipeline and routing application. Run the validation steps below before describing the system as successfully deployed or production-ready. This is a student/demo project, not a production navigation service.

## Architecture

```text
                    TomTom Traffic Flow API
                              |
                              v
                   02_produce_to_kafka.py
                              |
                         Kafka topic
                         traffic-raw
                              |
                              v
                  03_fake_data_generator.py <---- Pune road graph
                              |
                         Kafka topic
                      traffic-enriched
                              |
                              v
                    06_edge_weights.py
                  Spark Structured Streaming
                  - 2-minute event-time window
                  - average segment speeds
                  - citywide congestion ratio
                              |
                              v
                    edge_weights.json
                              |
                              v
                 07_routing_api.py (FastAPI)
                  - loads OSM road graph
                  - refreshes traffic weights
                  - A* route calculation
                  - geocoding via Nominatim
                              |
                              v
                   Leaflet web interface

          Airflow: periodic Kafka, freshness and quota checks
```

### Data flow

1. **Ingestion:** `02_produce_to_kafka.py` polls TomTom for configured Pune corridors and publishes observations to Kafka topic `traffic-raw`.
2. **Road network:** `04_pune_road_graph.py` downloads the Pune driving network from OpenStreetMap using OSMnx and saves it as GraphML.
3. **Enrichment:** `03_fake_data_generator.py` caches the latest real corridor observations and generates synthetic readings on nearby real road edges. It publishes these to `traffic-enriched`. If the road graph is unavailable, it falls back to jittered coordinates; those records are marked as not being on real road segments.
4. **Stream processing:** `06_edge_weights.py` reads enriched events with Spark Structured Streaming, applies a two-minute event-time window, and calculates average current/free-flow speeds per segment. It also calculates a reading-count-weighted citywide congestion ratio.
5. **Routing:** The Spark job atomically writes `docs/samples/edge_weights.json`. `07_routing_api.py` periodically reads this file and updates edge travel-time weights on the OSM road graph. It exposes geocoding and route endpoints and serves the Leaflet frontend.
6. **Monitoring:** Airflow runs periodic checks for Kafka topic activity, edge-weight freshness and estimated TomTom request usage. Docker Compose manages the always-on producer, generator, Spark edge-weight job and API services.

## Technology stack

- **Streaming and messaging:** Apache Kafka (KRaft), Kafka-Python
- **Stream processing:** Apache Spark 3.5, PySpark, Structured Streaming
- **Traffic source:** TomTom Traffic Flow API
- **Geospatial data:** OpenStreetMap, OSMnx, NetworkX
- **Backend/API:** Python, FastAPI, Uvicorn
- **Frontend:** HTML, CSS, JavaScript, Leaflet
- **Orchestration and runtime:** Docker, Docker Compose, Apache Airflow
- **Geocoding:** OpenStreetMap Nominatim

## Repository map

| Path | Purpose |
|---|---|
| `01_fetch_tomtom.py` | One-request TomTom API connectivity check |
| `02_produce_to_kafka.py` | Polls live corridor traffic and publishes to `traffic-raw` |
| `03_fake_data_generator.py` | Enriches cached readings with synthetic road-segment events |
| `04_pune_road_graph.py` | Downloads and saves the Pune driving graph |
| `05_spark_streaming.py` | Optional basic Kafka-to-Spark console inspection script |
| `06_edge_weights.py` | Main Spark streaming aggregation and edge-weight output |
| `07_routing_api.py` | FastAPI routing, geocoding and frontend serving |
| `dags/health_check_dag.py` | Airflow pipeline health and quota checks |
| `frontend/` | Leaflet-based route-planning interface |
| `docker-compose.yml` | Kafka, producer, generator, Spark, API and Airflow services |

## Prerequisites

- Docker Desktop with Docker Compose v2
- A TomTom API key with access to the Traffic Flow Segment Data API
- Internet access for TomTom, OpenStreetMap graph download and Nominatim geocoding
- Sufficient memory and disk space for Kafka, Spark, Airflow and the road graph

## Configuration

Create a local `.env` file in the repository root. Do not commit it.

```dotenv
TOMTOM_API_KEY=replace_with_your_tomtom_api_key
TOMTOM_POLL_INTERVAL_SECONDS=460
GENERATOR_FIRE_INTERVAL_SECONDS=10
```

The Compose file loads `.env` for the application services. Keep credentials out of source code, screenshots, logs and Git history. If a credential has ever been committed, revoke/rotate it; deleting it in a later commit does not remove it from Git history.

The producer calculates an estimated daily TomTom request count from the number of watched corridors and polling interval. The current defaults are intended to stay near the stated free-tier request budget; check your own TomTom plan and the startup warning before running continuously.

## Run locally

Run these commands from the repository root.

### 1. Configure the environment

Create the `.env` file as described above.

### 2. Build the Pune road graph first

The graph is a required input for road-aligned synthetic enrichment and the routing API.

```bash
docker compose build app
docker compose run --rm app python 04_pune_road_graph.py
```

This downloads the selected Pune driving-area graph and writes:

```text
docs/samples/pune_road_graph.graphml
```

Do not proceed until the graph-generation command completes successfully and the file exists.

### 3. Start the services

```bash
docker compose up -d --build
docker compose ps
```

The Compose stack includes:

| Service | Role | Local endpoint |
|---|---|---|
| `kafka` | Kafka broker in KRaft mode | `localhost:9092` |
| `api` | Routing API and frontend | `http://localhost:8000` |
| `spark` | Optional interactive Spark container | `http://localhost:8080` |
| `airflow` | Scheduled health checks | `http://localhost:8081` |
| `producer` | TomTom-to-Kafka producer | Background service |
| `generator` | Synthetic enrichment producer | Background service |
| `edge-weights` | Main Spark streaming aggregation | Background service |

The `app` service is a utility container for running one-off scripts. The main pipeline services start automatically through Compose.

### 4. Verify the pipeline

Check container state and logs:

```bash
docker compose ps
docker compose logs --tail=100 kafka producer generator edge-weights api airflow
```

Check API health and the frontend:

```bash
curl http://localhost:8000/health
```

Open `http://localhost:8000` in a browser to load the frontend. The API health response should report the loaded road graph. Its `degraded` field indicates whether a fresh edge-weight output is available.

Check that Spark has produced the output file:

```text
docs/samples/edge_weights.json
```

Check Airflow at `http://localhost:8081`. The Compose configuration currently sets the standalone Airflow login to `admin` / `admin`; change this before exposing the service beyond a local development environment.

### 5. Stop the stack

```bash
docker compose down
```

To also delete the Kafka data volume:

```bash
docker compose down -v
```

The second command permanently removes the local Kafka volume data.

## Useful development commands

Test the TomTom API key independently:

```bash
docker compose run --rm app python 01_fetch_tomtom.py
```

Run the optional Spark console inspection script:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  05_spark_streaming.py
```

View logs for an individual service:

```bash
docker compose logs -f producer
docker compose logs -f generator
docker compose logs -f edge-weights
docker compose logs -f api
```

## Design notes and limitations

- **Real and synthetic data:** TomTom readings are real API observations for selected corridors. The generator creates synthetic readings for nearby road segments using a time-of-day congestion model. Synthetic records are explicitly marked with `simulated: true`; do not present them as real sensor or TomTom observations.
- **Coverage:** Direct TomTom coverage is limited to a small set of watched corridors. The routing service estimates weights for other roads using a citywide congestion ratio. This is a simplified approximation, not street-level live traffic coverage across all of Pune.
- **Windowing:** Spark uses event-time windows and a watermark. The routing output is refreshed as streaming batches are processed; it is not a hard real-time guarantee.
- **Routing:** NetworkX A* uses travel-time edge weights derived from the current traffic output, with default road-class speeds as fallback values.
- **Geocoding:** Nominatim has a usage policy and rate limits. This demo is intended for light testing, not high-volume public use.
- **Authentication and CORS:** The current demo login is username-only and is not secure authentication. CORS is permissive. Do not expose this API publicly or use it for real accounts without implementing proper authentication, authorization, rate limiting and restrictive CORS.
- **Operational readiness:** A successful container start is not sufficient proof of a working end-to-end pipeline. Validate Kafka message flow, Spark output freshness, API health and route responses before claiming a successful deployment.

## Security

Never commit `.env`, API keys, passwords, private keys, tokens, certificates or real user data. Review the repository history and rotate any credential that was previously exposed. Keep local secrets in ignored environment files or a secrets manager.

## Future improvements

- Add automated tests for ingestion, enrichment, aggregation and routing.
- Add structured logging, metrics, alerting and retry/backoff policies.
- Improve traffic coverage and map TomTom road classifications to OSM road classes.
- Persist processed events and historical traffic metrics for analysis.
- Add secure authentication, authorization and restrictive CORS.
- Add CI checks for formatting, linting, syntax and tests.
- Publish reproducible screenshots and a short end-to-end demo after validating the complete stack.

## Disclaimer

TransitPulse is an educational engineering prototype. Route estimates, synthetic traffic values and geocoding results are not guaranteed to reflect actual road conditions. Do not use it for safety-critical navigation.
