# fraud-stream-pipeline

A real-time transaction stream-processing pipeline built to demonstrate production-grade
streaming semantics end to end **windowed aggregation, event-time watermarking, late-data
handling, and exactly-once processing** on **Kafka → Spark Structured Streaming → Delta Lake**,
running locally via Docker Compose.

The domain is transaction / fraud-monitoring-style data (per-user activity aggregated over time
windows). The focus of the project is the **streaming correctness guarantees**, each of which is
not just implemented but *demonstrated with a falsifiable test* : see [DESIGN.md](DESIGN.md).

**Stack:** Kafka 3.9.0 (KRaft) · Spark 3.5.3 · Delta Lake 3.3.2 · Docker Compose · Python 3.8

---

## What this demonstrates

- **Streaming ingestion** - synthetic transaction producer → Kafka topic `transactions`.
- **Event-time windowing** - 1-minute tumbling windows, per-user counts via Spark Structured Streaming.
- **Watermarking & late-data handling** - 2-minute watermark; records arriving after their window
  has been finalized and evicted are **dropped**, proven with a control (on-time record persists) vs.
  test (too-late record dropped) and verified via `numRowsDroppedByWatermark`.
- **Exactly-once processing** - proven with a hard kill + restart test: no duplicate windowed rows
  and continuous offset commits across a mid-stream crash. Guarantee comes from **checkpoint-based
  deterministic offset replay** + **Delta transaction-log idempotency** (batch-ID re-commit is a no-op).
- **Storage** - append-only Delta Lake table with checkpointed, recoverable state.
- **Observability** - a `StreamingQueryListener` surfaces per-batch watermark, max event-time, and
  dropped-row counts.

## Architecture

<img width="1536" height="1024" alt="Architecture: producer to Kafka to Spark Structured Streaming to Delta Lake" src="https://github.com/user-attachments/assets/912a9008-ab76-46ab-abee-4209160a2374" />

Data flow: **Python producer** emits JSON transactions (1/sec) → **Kafka** `transactions` topic →
**Spark Structured Streaming** parses, applies watermark + tumbling window, aggregates per user →
**Delta Lake** sink (append mode, 30s processing-time trigger, checkpointed).

## Repository layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Local stack: Kafka (KRaft), Kafka UI, Spark + Jupyter |
| `app/producer.py` | Synthetic transaction producer (1 txn/sec → Kafka) |
| `app/late_trxn.py` | Injects a single back-dated event to demonstrate the too-late drop |
| `app/spark_read.py` | Stage 1 - raw Kafka → Delta (prove the wire) |
| `app/spark_parse.py` | Stage 2 - typed parse of the JSON payload |
| `app/spark_window.py` | Stage 3 - watermark + tumbling-window aggregation → Delta (the main job) |
| `app/read_windowed.py` | Reads the windowed Delta output (used for the drop / exactly-once checks) |
| `app/read_parsed.py`, `app/read_delta.py` | Helper readers for intermediate tables |
| `app/spark-defaults.conf` | Bakes the Kafka + Delta packages so no `--packages` at submit time |
| `RUNBOOK.md` | Step-by-step setup commands, with the reasoning behind each |
| `DESIGN.md` | Design rationale: architecture, watermark trade-offs, exactly-once, failure modes, scaling |

## Running locally

See **[RUNBOOK.md](RUNBOOK.md)** for the full setup sequence. In short:

```bash
docker compose up -d          # bring up Kafka + Spark
# create the topic, then:
docker compose exec --user root spark python3 /opt/app/producer.py        # start producing
docker compose exec spark /opt/spark/bin/spark-submit /opt/app/spark_window.py   # start the stream
```

## Design & rationale

The engineering decisions - why a 2-minute watermark, why append mode, how exactly-once is
guaranteed and proven, failure modes, and how this would scale 100× - are written up in
**[DESIGN.md](DESIGN.md)**.

## Scope

This is a local, single-broker development build focused on demonstrating streaming *semantics*,
not a production deployment. Producer and Spark share a container for dev convenience; partitioning,
multi-broker replication, and a real fraud-scoring model are out of scope by design and discussed as
future work in DESIGN.md.