# Architecture Decision Record: Stage 3 Streaming Extension

## Status
Accepted - Stage 3 Implementation

## Context
Stage 3 introduces a real-time streaming requirement: process 12 micro-batch transaction files (2,415 total events) and maintain:
1. **current_balances** - Live account balances updated within 5-minute SLA
2. **recent_transactions** - Last 50 transactions per account

This extends the existing Stage 2 batch pipeline that processes 300k accounts, 3.15M transactions, and 240k customers.

## Decision

### 1. How Stage 1 Architecture Facilitated or Hindered Streaming

**What helped:**

| Design Choice | How It Helped |
|---|---|
| **DuckDB as processing engine** | Lightweight, no Spark overhead, could run streaming in same container |
| **Parquet as storage format** | Columnar format works for both batch and streaming |
| **Separation of concerns (ingest/transform/provision)** | Added `stream_ingest.py` without touching batch code |
| **Configuration externalization** | Easy to add stream paths without code changes |
| **Gold layer as source of truth** | Batch `dim_accounts` provided reliable starting balances |

**What created friction:**

| Friction Point | Impact |
|---|---|
| **No state management** | Had to implement file tracking from scratch (processed_files set) |
| **Monolithic run_all.py** | Required modification to add streaming stage |
| **No incremental processing** | Streaming depends on full batch completion |
| **No checkpoint system** | Requires reprocessing all stream files on each run |

**Code Survival Rate:** ~85% of Stage 1/2 code remained unchanged. Only `run_all.py` was modified; `stream_ingest.py` was added as new module.

### 2. Design Decisions in Stage 1 I Would Change in Hindsight

| Original Design | What I'd Change | Why |
|---|---|---|
| **Hardcoded layer sequence in `run_all.py`** | Config-driven pipeline stages (YAML list) | Adding streaming would just be config change |
| **No processed file tracking** | Checkpoint table from Day 1 | Streaming file tracking would be reusable |
| **Batch gold layer as final output only** | Add `last_updated` timestamps to all gold tables | Would simplify SLA monitoring |
| **No balance state table** | Maintain `current_balances` from Day 1 (even if batch-only) | Streaming would just update existing structure |
| **Single entry point assumption** | Support multiple entry points (batch/stream) | Would make integration cleaner |

### 3. Day 1 Architecture with Stage 3 Knowledge

If I knew Stage 3 was coming from the start, I would have built:

**Ingestion Pattern:**
- Unified ingestion layer supporting both batch files (`/data/input`) and streaming micro-batches (`/data/stream`)
- Common schema validation before routing to batch vs stream paths

**State Management:**
- DuckDB-based checkpoint table tracking processed files and watermarks
- Idempotent processing from Day 1

**Output Structure:**
- `current_balances` table from Day 1 (updated by batch, then incrementally by stream)
- `recent_transactions` with window retention (50 per account) from start
- All gold tables include `last_updated` timestamp

**Entry Point Design:**
- Config-driven pipeline: `pipeline:
    stages:
      - name: batch_ingest
      - name: batch_transform  
      - name: batch_provision
      - name: stream_ingest`
- Supports adding/removing stages without code changes

**Processing Engine:**
- Keep DuckDB (already memory-efficient)
- Add support for both full refresh (batch) and incremental (streaming) modes

**Trade-offs Accepted:**
- Simpler polling (vs event-driven) - sufficient for 5-minute SLA
- Same engine for batch and stream - reduces complexity
- Parquet files maintainable under 2GB memory constraint

## Consequences

### Positive Outcomes
- ✅ Streaming works within 2GB memory limit
- ✅ SLA met (all events processed in < 5 minutes from file arrival)
- ✅ Batch pipeline unchanged and still passes Stage 1/2 validation
- ✅ Gold layer provides source of truth for streaming
- ✅ New tables (`current_balances`, `recent_transactions`) support mobile app requirements

### Known Limitations
- Requires full batch completion before streaming starts
- No exactly-once delivery (files may be reprocessed on restart)
- Polling (file scan) vs event-driven adds minimal latency (acceptable for SLA)

## Metrics (Stage 3 Run)
- Batch processing: 300k accounts, 3.15M transactions, 240k customers
- Stream processing: 2,415 events across 12 files
- Total run time: 61 seconds (well within 30-minute limit)
- Memory usage: ≤ 2GB
- Streaming output: 2,399 accounts updated, recent transactions table written

## Date
2026-05-02

## Authors
Data Engineering Challenge Participant
