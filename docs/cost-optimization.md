# Cost Optimization — Financial Risk & Fraud Detection Platform

> This document tracks BigQuery optimization experiments, cost analysis, and cost reduction strategies implemented across the platform.
> Experiments are added as each optimization is implemented (Phase 11).

---

## 1. BigQuery Cost Model

BigQuery on-demand pricing:
- **Storage**: ~$0.02/GB/month (active), ~$0.01/GB/month (long-term)
- **Queries**: ~$5/TB scanned (on-demand)
- **Streaming inserts**: ~$0.01/200MB

With 500M transactions × ~500 bytes each ≈ 250 GB of raw transaction data.
At full table scan: 250 GB × $5/TB = **$1.25 per query** (at scale: 100 queries/day = $125/day).
After partitioning + clustering: typical query scans <1% of data → **~$0.01 per query**.

---

## 2. Experiment 1 — Partitioning vs. No Partitioning

*(To be completed in Phase 11)*

### Setup
Same query run against:
- `core.fact_transaction_unoptimized` — no partition, no cluster
- `core.fact_transaction` — partitioned by `transaction_date`, clustered by `customer_key`, `merchant_key`

### Query
```sql
SELECT
    COUNT(*) as txn_count,
    SUM(amount_usd) as total_volume,
    AVG(risk_score) as avg_risk_score
FROM <table>
WHERE transaction_date = '2024-06-15'
  AND customer_key IN (SELECT customer_key FROM core.dim_customer WHERE country = 'US')
```

### Results

| Metric | Unoptimized | Optimized | Improvement |
|---|---|---|---|
| Bytes scanned | (TBD) | (TBD) | (TBD) |
| Query duration | (TBD) | (TBD) | (TBD) |
| Estimated cost | (TBD) | (TBD) | (TBD) |

---

## 3. Experiment 2 — Incremental vs. Full Reload

*(To be completed in Phase 5)*

### Problem
Naive approach: DELETE + full INSERT every day = scans entire table.
Optimized approach: MERGE on `transaction_id` using only new records.

### Results

| Metric | Full reload | Incremental MERGE | Improvement |
|---|---|---|---|
| Rows scanned | (TBD) | (TBD) | (TBD) |
| DML cost | (TBD) | (TBD) | (TBD) |
| Duration | (TBD) | (TBD) | (TBD) |

---

## 4. Cost Reduction Strategies Implemented

### 4.1 BigQuery
- [ ] Partition all fact tables by date column
- [ ] Cluster by most-frequently-filtered columns
- [ ] Use `INFORMATION_SCHEMA.JOBS` to monitor query costs
- [ ] Set per-project and per-user query cost controls
- [ ] Use materialized views for expensive repeated aggregations

### 4.2 GCS
- [ ] Lifecycle policy: move to Nearline after 30 days, Coldline after 90 days, delete after 365 days (for `raw/`)
- [ ] Archive processed files after successful pipeline run
- [ ] Compress CSV files (gzip) before upload

### 4.3 Dataflow
- [ ] Use autoscaling (enabled by default)
- [ ] Use Spot/Preemptible VMs for batch jobs (60–80% cheaper)
- [ ] Avoid always-on streaming where batch suffices
- [ ] Profile and tune DoFn hot paths

### 4.4 Pub/Sub
- [ ] Use snapshot + seek for replay (no duplicate topics)
- [ ] Set message retention to 7 days (not 31)

### 4.5 Cloud Run
- [ ] Scale-to-zero (serverless — no idle costs)
- [ ] Set minimum instances = 0 (dev/staging), minimum instances = 1 (prod to avoid cold start)

---

## 5. Monthly Cost Estimate (Phase 4+ GCP)

| Service | Usage (estimated) | Monthly Cost |
|---|---|---|
| BigQuery storage | 250 GB active | ~$5 |
| BigQuery queries | 50 queries/day × 1 GB avg | ~$7.50 |
| GCS storage | 500 GB across all prefixes | ~$10 |
| Dataflow batch | 2 hrs/day × 4 workers | ~$15 |
| Dataflow streaming | 8 hrs/day × 2 workers | ~$20 |
| Cloud Run | 100k requests/day | ~$2 |
| Cloud Composer | 1 small environment | ~$100 |
| Pub/Sub | 500k messages/day | ~$2 |
| **Total** | | **~$162/month** |

> Note: Cloud Composer is the dominant cost. For development/portfolio purposes, you can use the Astronomer CLI locally and only deploy to Cloud Composer when demonstrating the full GCP architecture.
