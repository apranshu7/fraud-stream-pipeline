# fraud-stream-pipeline

Real-time transaction fraud detection pipeline.
Built on Kafka, Spark Structured Streaming, and Delta Lake — all running locally via Docker.

🚧 Work in progress — currently bootstrapping infrastructure.

## What this demonstrates
- Streaming ingestion via Kafka
- Real-time processing via Spark Structured Streaming
- Stateful aggregations with tumbling windows and watermarking
- Exactly-once semantics via checkpointing
- Storage on Delta Lake

## Architecture
<img width="1536" height="1024" alt="ChatGPT Image Jun 21, 2026, 11_50_23 PM" src="https://github.com/user-attachments/assets/912a9008-ab76-46ab-abee-4209160a2374" />


## Running locally
- [Runbook](RUNBOOK.md) — step-by-step setup commands with reasoning
