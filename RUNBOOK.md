# fraud-stream-pipeline — Setup Runbook

Commands run to stand up the pipeline, in order, with the reasoning behind each.
Shell is **PowerShell on Windows**, so multi-line commands are written on one line
(PowerShell uses backtick `` ` `` for continuation, not `\`).

Architecture target: Python producer → Kafka topic `transactions` →
Spark Structured Streaming → Delta Lake. Built incrementally; this file tracks
what's actually been done, not the end-state.

---

## 0. Pinned stack (verified against docs — do not drift)

- Spark **3.5.3**, Scala **2.12.18**, Java 11 (container Python is **3.8.10**)
- Submit-time packages (both Scala 2.12):
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
  - `io.delta:delta-spark_2.12:3.3.2`  (Delta 3.3.2 is built on Spark 3.5.3; package is `delta-spark`, not the old `delta-core`)
- Delta SparkSession configs (needed once the Spark job is written):
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- Kafka broker advertises internally as `kafka:9092` (single listener).
  - From inside the compose network → use `kafka:9092`.
  - `localhost:9092` only works from the host, NOT from inside another container.

---

## 1. Bring the stack up / check status

```powershell
docker compose up -d
docker compose ps
```

Expect all 3 services (Kafka broker, Kafka UI, Spark) showing `Up`.

Daily startup: `up -d` → `ps` (confirm all Up) → proceed.
Bring down for the night with `docker compose down` (data in named volumes survives).
**Never `down -v`** once checkpoints matter — `-v` deletes named volumes, including
`spark_checkpoints`. That's the foot-gun.

---

## 2. Confirm container Python version

```powershell
docker compose exec spark python3 --version
```

Returned `Python 3.8.10`. Matters because:
- 3.8 → plain `kafka-python` is fine (the 3.12 breakage that needs the `kafka-python-ng`
  fork does NOT apply here).
- Keep producer code 3.8-clean (no `match`, no `dict | dict` merge syntax).

---

## 3. Install the Kafka client library

First attempt failed: `[Errno 13] Permission denied: '/home/spark'`
(container runs as non-root `spark` user without write access).

Fix that worked — install as root:

```powershell
docker compose exec --user root spark pip install kafka-python
```

Ends with `Successfully installed kafka-python-X.X.X`.

⚠️ This install lives only in the running container — it is **lost on `docker compose down`**
(container gets recreated). For persistence, bake it into a Dockerfile / `requirements.txt`
later. Fine for dev iteration now.

Because it was installed as root, **run the producer as root too** (see step 6) so the
package is importable.

---

## 4. Find the Kafka CLI location (one-time probe)

```powershell
docker compose exec kafka bash -c "ls /opt/kafka/bin/ 2>/dev/null || ls /opt/bitnami/kafka/bin/ 2>/dev/null || ls /usr/bin/ | grep kafka"
```

`||` runs each branch only if the previous failed. Full unfiltered listing came back →
scripts live at **`/opt/kafka/bin/`**. (`kafka-topics.sh` confirmed present there.)

For zero-inference path lookup in future: `which kafka-topics.sh` inside the container.

---

## 5. Create the `transactions` topic explicitly

Done deliberately rather than relying on `auto.create.topics.enable` — explicit topic
creation is the production-correct habit (you pick partition count; typos don't silently
create junk topics).

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

- `--partitions 1` — plenty at laptop scale; raise in prod for parallelism/consumer scaling.
- `--replication-factor 1` — only one broker, so >1 would fail (nowhere to put replicas).

(Also creatable via Kafka UI → Topics → Add a Topic. Same result. CLI/IaC is the
prod-correct route; clicking doesn't version-control.)

Verify it exists:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
```

`transactions` should appear.

---

## 6. Run the producer

Producer code lives at `app/producer.py` (bind-mounted to `/opt/app/producer.py`
in the container). Generates one synthetic transaction per second and sends JSON
to the `transactions` topic.

Run as root (matches the root install in step 3):

```powershell
docker compose exec --user root spark python3 /opt/app/producer.py
```

Expect: one `sent: txn_... user_... $...` line per second in the terminal.
Stop cleanly with **Ctrl-C** → `Stopping producer...` → `Producer closed.`
(The clean shutdown calls `producer.flush()` so buffered messages aren't lost.)

---

## 7. Verify data is actually on the wire

Terminal `sent:` lines only prove the script *queued* messages. Confirm they LANDED:

Kafka UI → http://localhost:8080 → Topics → `transactions` → **Messages** tab.

Should see JSON transactions accumulating with sequential offsets, ~1s-apart timestamps.

Notes from first successful run:
- `Timestamp type: CREATE_TIME` → producer-set timestamp (vs `LOG_APPEND_TIME` = broker
  arrival). Distinct from the `event_time` field inside the JSON payload — watermarking
  later keys off the payload `event_time`, not Kafka's record timestamp.
- Key is empty (value-only sends). Fine now. Later, keying by `user_id` would route a
  user's events to the same partition (matters for per-user ordering once partitions > 1).

---

## Status / next step

- ✅ Kafka broker + Kafka UI + Spark engine standing & verified
- ✅ `transactions` topic created
- ✅ Producer built and verified — data flowing to Kafka
- ⏭️ **NEXT:** trivial Spark read — Kafka → Delta *raw*, no logic. Prove the
  Spark↔Kafka↔Delta wire end-to-end BEFORE adding windowing/watermark/exactly-once.

---

## Useful one-liners (reference)

```powershell
# List topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092

# Describe a topic (partitions, replication, config)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic transactions --bootstrap-server kafka:9092

# Tail messages from the CLI (alternative to Kafka UI)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic transactions --bootstrap-server kafka:9092 --from-beginning

# Check Spark version inside the container
docker compose exec spark spark-submit --version

# Install packages and submit the spark read job
docker compose exec --user root spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.3.2 /opt/app/spark_read.py
```
## Windowing persistence - DONE (03-Jul)

Windowed aggregation now persists to Delta. `spark_window.py`:
`to_timestamp(event_time)` → `withWatermark("event_time","2 minutes")` →
`groupBy(window(event_time,"1 minute"), user_id).count()` →
`writeStream.format("delta").outputMode("append")
.trigger(processingTime="30 seconds")`
path `/opt/delta/transactions_running_prod`,
checkpoint `/opt/checkpoints/running_prod_checkpoint`.

**Why the earlier append run wrote 0 rows (and this one doesn't):**
- `availableNow` drained the bounded backlog and exited → max event-time
  froze → watermark (max − 2 min) froze → never passed any window's end →
  no window finalized → append wrote nothing. Correct watermark behavior,
  not a bug.
- Fix: live producer + `processingTime` trigger. Live events keep pushing
  max event-time forward → watermark advances → windows finalize and append.
  `processingTime` keeps the query alive and re-checking; `availableNow` is
  right for stateless raw/parse, wrong for the stateful windowing layer.
- First rows land ≈ window length (1 min) + watermark lag (2 min) after the
  producer starts, rounded up to the next trigger tick — NOT 2 min flat, and
  NOT immediately. Do not Ctrl-C early thinking it's the zero-row bug again.

**Verified:** 441 windowed rows in Delta. Schema flattened to
`start_time, end_time, user_id, count` (window struct unpacked). Counts 1–3
per (user, window) as expected.

**Run:** producer in terminal 1
(`docker compose exec --user root spark python3 /opt/app/producer.py`),
spark-submit `spark_window.py` in terminal 2 with the kafka + delta packages
and Delta SparkSession configs. Both run forever; Ctrl-C each to stop.
`awaitTermination()` MUST be the last line or the async query dies on exit.

**Known local-dev noise:** Parquet `MemoryManager ... scaling row group
sizes` WARNs under small container heap. Self-correcting, not an error.

⏭️ NEXT: LATE/DUP injection in producer → too-late drop demo; then
exactly-once kill+restart proof. Persistence is done; these two are not.
