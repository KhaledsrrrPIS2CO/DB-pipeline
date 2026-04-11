# DB Timetables Quality Monitor

Calls the Deutsche Bahn Timetables API every 15 minutes.
Scores how many records are complete. Stores that score permanently. Alerts when it drops.

```
quality_score = complete_records / total_records × 100
```

A record is complete if it has: station, train name, planned departure, actual departure, delay, cancellation status.

---

## Architecture

```
DB Timetables API (Hamburg Hbf)
    ↓  every 15 min · XML
Lambda + EventBridge
    ↓  raw XML, immutable
S3 Bronze  ·  partitioned by date/hour
    ↓
Glue Spark  ·  parse XML · score quality per batch
    ↓  quality_score + is_complete per record
S3 Silver  ·  Apache Iceberg  ·  time-travel · full audit history
    ↓
Athena  ·  serverless SQL        CloudWatch  ·  alert if score < 60%
```

---

## Stack

| Tool | Why |
|---|---|
| Lambda + EventBridge | 4 API calls/hour. Kinesis costs $15/month idle. Lambda is free. |
| S3 Bronze | Raw = source of truth. Silver breaks? Reprocess from here. |
| Apache Iceberg | Time-travel + upserts on S3. AWS-native. Zero extra cost. |
| Glue Spark | Serverless. $0.15/run. Native Iceberg support. |
| Athena | $5/TB scanned. Silver is a few GB. Each query = fractions of a cent. |
| OpenTofu | One command to deploy. One to destroy. Every resource in code. |
| Prefect | Free tier. Python-native. Statista JD requires it. |
| GitHub Actions | Every push triggers `tofu apply`. Auditable. |

---

## Deploy

```bash
git clone git@github.com:KhaledsrrrPIS2CO/DB-pipeline.git
cd DB-pipeline/infra
tofu apply       # entire stack live
tofu destroy     # after every session
```

**Cost: ~$3/month. Entirely serverless.**

---

## Query

```sql
-- Quality score by hour, today
SELECT ingestion_hour, AVG(quality_score) AS avg_quality
FROM silver.train_quality
WHERE ingestion_date = current_date
GROUP BY ingestion_hour
ORDER BY ingestion_hour;

-- What did quality look like 30 days ago?
SELECT ingestion_hour, quality_score
FROM silver.train_quality
FOR SYSTEM_TIME AS OF (current_timestamp - interval '30' day);
```

---

## Transferability

Same pattern. Different domain.
When 3 of 10 hospital records are missing a diagnosis code — you need to know. And prove when it started.