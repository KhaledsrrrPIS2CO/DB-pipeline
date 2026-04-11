# Deutsche Bahn Delay Lakehouse

## Problem
DB delay records arrive late and incomplete.
Dashboards show wrong data. Decisions are made on lies.

## What this pipeline does
- Ingests monthly delay data from Deutsche Bahn
- Detects missing and null records
- Stores data correctly in Apache Iceberg on S3
- Alerts when data quality drops
- Full audit trail via Iceberg time-travel

## Stack
| Layer | Tool |
|---|---|
| Storage | Apache Iceberg on S3 |
| Processing | AWS Glue + Spark |
| Query | Amazon Athena |
| Orchestration | Prefect |
| IaC | OpenTofu |
| CI/CD | GitHub Actions |

## Cost
Under $5/month at development scale.

## Deploy
```bash
tofu apply
```

## Transferable to
Healthcare KPI pipelines, logistics tracking, financial reporting.
Any domain where late data corrupts decisions.