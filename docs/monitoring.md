# Monitoring — Financial Risk & Fraud Detection Platform

> This document covers infrastructure monitoring, data pipeline health monitoring, alerting policies, and SLA definitions.
> Full implementation is in Phase 10. This file is the design specification.

---

## 1. Monitoring Strategy

Monitoring is split into two categories:

| Category | What | Tools |
|---|---|---|
| **Infrastructure** | Dataflow failures, Pub/Sub backlog, Cloud Run errors, Composer failures | Cloud Monitoring + Cloud Logging |
| **Data quality** | Record counts, freshness lag, null rates, duplicate rates, DQ errors | Custom metrics + Great Expectations |

---

## 2. Infrastructure Metrics

### Dataflow
| Metric | Alert Threshold | Action |
|---|---|---|
| Job failure | Any failure | Page on-call |
| Elements behind watermark | > 100,000 elements | Warn |
| Worker CPU utilization | > 85% for 10 min | Scale up |
| Data freshness | > 30 min lag | Warn |

### Pub/Sub
| Metric | Alert Threshold | Action |
|---|---|---|
| `subscription/num_undelivered_messages` | > 10,000 | Warn — pipeline may be falling behind |
| `subscription/oldest_unacked_message_age` | > 5 min | Critical — investigate Dataflow job |
| Dead-letter topic message count | > 0 | Warn — bad messages being produced |

### Cloud Run
| Metric | Alert Threshold | Action |
|---|---|---|
| HTTP 5xx error rate | > 1% | Critical |
| Request latency P99 | > 2s | Warn |
| Container instance count | > 10 | Warn (unexpected load spike) |

### Cloud Composer (Airflow)
| Metric | Alert Threshold | Action |
|---|---|---|
| DAG run failure | Any failure | Notify team |
| Task retry count | > 3 per task | Investigate |
| SLA miss | Any | Notify team lead |

---

## 3. Data Quality Metrics

These are custom metrics written to Cloud Monitoring from the DQ pipeline.

| Metric | Description | Alert |
|---|---|---|
| `custom/dq/null_rate` | % of null values in key columns | > 5% |
| `custom/dq/duplicate_rate` | % of duplicate `transaction_id` | > 0% |
| `custom/dq/referential_integrity_fail_rate` | % of transactions with missing FK | > 0.1% |
| `custom/dq/error_count` | Total DQ errors per pipeline run | > 100 |
| `custom/pipeline/freshness_lag_minutes` | Minutes since last record arrived | > 30 min |
| `custom/pipeline/volume_deviation_pct` | % deviation from expected daily volume | > 20% |

---

## 4. Alerting Channels

| Channel | Use Case |
|---|---|
| Email | Non-urgent warnings |
| (Optional) Slack webhook | Team notifications |
| PagerDuty / On-call | Critical pipeline failures |

Alert policies are defined as code in `monitoring/alert_policies/`.

---

## 5. Dashboards

Two Cloud Monitoring dashboards:

### Infrastructure Dashboard
- Dataflow job status and throughput
- Pub/Sub backlog over time
- Cloud Run request rate and latency
- Composer DAG health

### Data Quality Dashboard
- DQ error rate over time
- Freshness lag per table
- Volume trends vs. baseline
- High-risk transaction rate

---

## 6. SLA Definitions

| Pipeline | SLA | Measurement |
|---|---|---|
| Daily batch load | Data available in BQ by 06:00 UTC | `freshness_lag_minutes` |
| Streaming risk score | Score available within 30 seconds of transaction | Event time → BQ write latency |
| DQ validation | Checks complete within 1 hour of data load | Airflow DAG completion time |
| Analytics mart refresh | Refreshed by 07:00 UTC | Mart table `max(ingestion_timestamp)` |

---

## 7. Log-Based Metrics

Key log patterns monitored:

```
# Dataflow pipeline errors
resource.type="dataflow_step"
severity=ERROR

# Data quality failures
jsonPayload.event_type="dq_check_failed"

# Dead-letter messages
resource.type="pubsub_topic"
resource.labels.topic_id="transaction-dead-letter"

# Cloud Run 5xx
resource.type="cloud_run_revision"
httpRequest.status>=500
```

---

## 8. Runbooks

*(To be completed in Phase 10)*

- `runbooks/dataflow-job-failure.md`
- `runbooks/pubsub-backlog-spike.md`
- `runbooks/dq-error-spike.md`
- `runbooks/missing-data-incident.md`
