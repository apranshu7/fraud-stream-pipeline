## 1. Problem & Architecture

This pipeline is a real-time monitoring substrate for a fraud-detection system. It ingests a stream of user transactions and computes per-user activity counts over short event-time windows the kind of rolling aggregation a downstream fraud model or alerting rule would sit on top of. The engineering focus is the streaming correctness guarantees (windowing, watermarking, exactly-once), not a fraud-scoring model.
Data flow: a synthetic Python producer emits one JSON transaction per second → Kafka (transactions topic) buffers and decouples ingestion from processing → Spark Structured Streaming parses, applies an event-time watermark and tumbling window, and aggregates per user → Delta Lake persists the windowed results as the sink.
Component choices:

Kafka (KRaft mode, no ZooKeeper) as the ingestion buffer the industry-standard durable log for real-time systems, and it decouples producer rate from consumer processing so a slow or restarting consumer doesn't lose data. Kafka UI is included for inspecting the topic.
Spark Structured Streaming for processing native Kafka and Delta integration, exactly-once support out of the box, and it lets the same DataFrame model I use for batch carry over to streaming (low marginal learning cost given a Spark background).
Delta Lake as the sink ACID transactions and a transaction log, which is what makes the exactly-once guarantee possible (idempotent commits) and gives recoverable, queryable output.

Key parameters: 1-minute tumbling windows, keyed on (window, user_id); a 2-minute event-time watermark; append output mode (each window is emitted once, after the watermark passes its end); a 30-second processing-time trigger. The window/watermark sizing reflects the turnaround a near-real-time fraud signal needs small enough to flag activity quickly, with enough watermark slack to absorb normal out-of-order arrival.

## 2. Windowing & the Watermark Trade-off
The pipeline aggregates transactions into 1-minute tumbling windows keyed on (window, user_id), producing a per-user transaction count for each minute. Windows are defined over the event's event_time, not its arrival time.

**The problem: late and out-of-order data**. Events do not arrive in event-time order a client may buffer transactions offline, reconnect, and dump older events minutes after they occurred. This forces a decision: when do we consider a window complete and finalize its count? Waiting indefinitely for stragglers is not an option it means never emitting a result and holding window state in memory forever. Not waiting at all means discarding legitimate late-but-valid data. The watermark is the mechanism that answers how long to wait.

How the watermark works. The watermark is defined as max(event_time seen so far) 2 minutes. It is monotonic (only ever moves forward) and data-driven: it advances only when an incoming event raises the running maximum event-time, and it freezes if the stream stalls or stops receiving newer events. A window is finalized and written to Delta only once the watermark passes that window's end at which point the window's state is evicted. The precise drop condition follows from this: a late record is dropped not simply because its event_time is below the watermark, but because its target window has already been finalized and evicted there is no longer any state for it to update.

The trade-off. The watermark size trades completeness against latency and state cost. A larger watermark captures more late-arriving data (higher completeness) but delays finalization (higher latency) and holds more window state in memory. A smaller watermark finalizes faster and leaner but drops more late data. For this use case near-real-time transaction monitoring, where a reasonably fresh signal matters 2 minutes is a defensible middle: enough slack to absorb normal out-of-order arrival, without stalling finalization long enough to blunt the real-time value.

Proving the drop behavior. The late-drop was verified with a falsifiable A/B test, holding the pipeline constant (1-min window, 2-min watermark) and varying only the lateness of an injected record. A control event injected ~1 minute late within the watermark tolerance correctly persisted in the output. A test event injected ~5 minutes late past its window's eviction was correctly dropped. The drop was confirmed two ways: the numRowsDroppedByWatermark metric (surfaced via a StreamingQueryListener overriding onQueryProgress, read from the progress event JSON) incremented, and a negative check confirmed the record was absent from the Delta output. (See spark_window.py / late_trxn.py.)

## 3. Exactly once Mechanics
To define exactly once mechanism in simple terms think of scenario where there's an abrupt termination of a stream mid commit of a particular batch of offsets say 100-150, here once the stream is continued the guarantee should be that a record read once from input should commit to the output exactly once , not get skipped ( at most once ) nor gets re-written (duplicates, at least once).
This is however different from duplication of data in the input itself that's a separate issue.

**The Exact Scenario**: Let's say your stream crashes ungracefully mid commit for a batch from offset 100-150, now upon restart you see the stream skipping over 100-150 and starts from 151, that is our at most once scenario here the write actually doesn't happen the first time but there's no way the stream can understand that and considers the batch processed. Another case could be that you notice the windowed output rows get re-appended for the 100-150 offsets (rows getting doubled) which is the at least once scenario, here upon restart the stream read the entire batch again considering it as unfinished, in reality the write commit already happened.

**Two mitigation mechanics** : Checkpointing and Delta idempotent commits are the two mechanics that ensure in spark streaming we have the exactly once scenario  implemented for any abrupt failures. The checkpoint basically ensures every batches offset metadata is logged before continuing with the actual data processing. When there's a failure and restart it re-reads the offsets logs to determine the processing plan. For Delta idempotent commits; the Structured Streaming stamps each micro-batch with a batch ID, and Delta's transaction log records which batch IDs have committed; on replay, Delta sees the batch ID already present in the log and skips the write entirely, this only works because Delta is ACID compliant (logs are atomic). Checkpointing alone prevents the at most once scenario and the idempotent commits prevent the atleast once ,  both are essential together to make sure we are getting the exactly once consistency.

To prove this, I basically had to test for two things upon restart there are no duplicates and the offset/ commit continuity is maintained i.e. no-loss scenario. To start off on an active stream i queried for counts per users per start and end time with a filter n > 1 where n is the count for the triple mentioned above. This shows zero records which means no duplicates. I also recorded the current offset/commit count to check for the no-loss scenario later. I kill the spark container ungracefully to induce abrupt termination in the stream, a gracefull stop like Ctrl + C won't work here. Once i restart the the container and the spark job i don't intentionally start the producer; with no new data arriving, the only work on restart is the replay of the pre-crash backlog, so any duplicate or gap is unambiguously attributable to recovery rather than new arrivals. Next, i query the triples again and check for n > 1 records which is zero hence no duplication happened also again i check for commit file counts which resumes from where it left off with continuity of last commit file number maintained.

**The Caveat for Testing**: Since this stream is working on synthetic producer data there is no way of knowing which particular data is expected in what batches and hence there is no way of ensuring no-loss continuity just by looking at the output itself. That's why the duplicate scenario we can check with eyeballing the output but for no-loss continuity I had to check the commit file counts.(See spark_window.py and the read query in read_windowed.py)

## 4. Failure Modes & Recovery
The exactly-once proof in §3 was demonstrated under one failure: a hard kill of the Spark container mid-stream. This section generalizes that result across the failure surface and states, for each mode, whether the pipeline recovers cleanly, degrades, or loses data.

**Driver/executor death mid-batch**

The kill test (§3) is the representative case. When Spark dies between reading a Kafka offset range and committing to Delta, no partial state is durably visible: the in-flight micro-batch's work exists only in executor memory and an uncommitted Delta write. On restart, Spark reads the checkpoint, identifies the last batch that was started but not committed, and replays it from the offsets recorded before the crash. Recovery is automatic and lands in the same logical position the guarantee proven empirically in §3, generalized to any process death mid-batch.

**The two logs behind exactly-once (the recovery crux)**

Exactly-once depends on two separate logs, kept by two different systems, cross-checked by a shared batch number. Conflating them is the usual source of confusion.
Log 1: the Spark checkpoint (running_final_checkpoint/). Spark's private bookkeeping of its own progress. It holds two folders:

offsets/N: "I intend to process this Kafka offset range as micro-batch N." Written before batch N runs.
commits/N: "micro-batch N finished." Written after it completes.

Here N is just the micro-batch number (0, 1, 2, …). In a healthy stream offsets/ is always exactly one ahead of commits/. These files say nothing about Delta directly they are Spark's to-do list with checkmarks.
Log 2 the Delta transaction log (_delta_log/ in transactions_running_final/). Delta's own independent record of which writes are committed to the table.

The link is the batch number. When Spark writes batch N to Delta, it stamps that write with the query ID (txnAppId) and batch number (txnVersion). Recovery then cross-checks the two logs:

Spark checks its own checkpoint: is there a commits/N? If not, batch N never finished replay it from the offsets in offsets/N. (Kafka ranges are start-inclusive, end-exclusive, so the replay consumes exactly the same records.)
The replayed batch writes to Delta again. Delta checks its own log: have I already committed a write stamped batch N? If yes → skip it (no duplicate rows). If no → accept it.

This handles the dangerous window the crash occurs after offsets/N is written but before commits/N lands, possibly with the Delta write already done and only the commit marker lost. Either way, replay resolves to exactly-once: if the write had completed, Delta skips the duplicate; if it hadn't, the replay finishes it.

Neither log alone is sufficient. Checkpoint replay without Delta idempotency gives at-least-once (duplicate rows on the replayed batch); Delta idempotency without the checkpoint has no way to know which batch to replay. The checkpoint tracks intent, Delta's log tracks completion, and together they behave like a distributed two-phase commit which is the actual mechanism behind the §3 kill-test result.

**Kafka-side failures**

Two cases. If the broker is simply down when Spark restarts, Spark retries until Kafka returns and resumes from the checkpoint no data lost, just delayed. The real risk is retention expiry: if the stream is offline long enough that Kafka deletes old records before Spark reads them, those records are gone. Spark resumes from the checkpoint, finds its saved position no longer exists, and continues from the oldest surviving record the gap in between is unrecoverable. This is the argument for setting Kafka retention longer than worst-case downtime. (Spark recovers strictly from its checkpoint, not from the Kafka consumer group offset, which it ignores on restart.)

**Delta-side failures**

A write that starts and then dies before its transaction-log entry commits leaves orphaned data files that no reader ever sees Delta readers resolve table state from the transaction log, and an uncommitted write has no log entry, so it is invisible. There is no half-written, partially-readable table state. The orphaned files are dead weight until vacuumed, not a correctness problem.

**Checkpoint loss**

The checkpoint is the only durable record of stream position (see 4.3); delete or corrupt it and the query restarts from scratch, rebuilding all aggregation state which is why docker compose down -v is banned once a stream is live.

**Known limitation malformed records handled silently**

A malformed record is any message on transactions the parse chain can't turn into a clean row: not valid JSON, a field of the wrong type (amount as "free"), a missing field, or an event_time that won't parse as a timestamp.

The pipeline doesn't reject these it lets them through as nulls. from_json runs in PERMISSIVE mode by default, so bad JSON becomes a null-filled row instead of an error, and to_timestamp returns null on an unparseable string (Spark 3.5, ANSI off) rather than throwing. A record with a null event_time can't be placed in any window, so it silently drops out of the aggregation no error, no log, no trace. The stream stays up while quietly losing data.

The fix is a dead-letter path: capture unparseable records (via columnNameOfCorruptRecord) and route null-event_time rows to a quarantine sink, so a bad message is a logged drop, not an invisible one.

*(This silent-null behavior is specific to Spark 3.5 with ANSI mode off. Under FAILFAST mode or Spark 4.0's ANSI-by-default, these throw instead turning silent drops into a hard batch failure the checkpoint replays as a crash loop.)*
