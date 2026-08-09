# TransitPulse-lite: minimal setup

Two containers only, on purpose:

- **kafka** — single-node Kafka in KRaft mode (no separate Zookeeper
  container needed — one less moving part than a classic Kafka setup).
- **app** — a plain Python container where your scripts run, via
  `docker compose exec app python <script>.py`.

No MySQL, no Spark, no Airflow yet. We add those only once each piece
before it is proven working with real output -- same discipline as the
NYC traffic-matching work, just applied to this pipeline.

## Setup

1. Edit `.env` and paste your real TomTom API key in place of
   `paste_your_key_here`.
2. Build and start:
   ```bash
   docker compose up -d --build
   ```
3. Confirm both containers are running:
   ```bash
   docker compose ps
   ```
   You should see `tp_kafka` and `tp_app` both `Up`/`running`. If either
   isn't, paste the output here before doing anything else -- no point
   building on top of a broken container.

## Next step

Once both containers show as running, that's the actual "Kafka is
working" confirmation we needed. From there we write and run:

1. `01_fetch_tomtom.py` — just prints live Pune traffic to console,
   no Kafka involved yet. Confirms the API key and response shape.
2. A Kafka producer script that publishes that data to a `traffic-raw`
   topic, verified with a console consumer.
3. The fake data generator, consuming `traffic-raw` and republishing
   to `traffic-enriched`.
4. Spark, added only once 1-3 are proven working.

## Useful commands

```bash
# See logs if something looks wrong
docker compose logs kafka
docker compose logs app

# Stop everything
docker compose down

# Full reset (wipes Kafka's stored data too)
docker compose down -v
```
