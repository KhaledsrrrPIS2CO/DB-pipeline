"""
glue_job.py — S3 Bronze → Quality Scoring → S3 Silver (Iceberg)

What this does:
  1. Reads raw XML from S3 Bronze
  2. Parses each train record
  3. Scores quality: complete_records / total_records × 100
  4. A record is complete if all 6 fields are present:
     station, train_name, planned_departure, actual_departure,
     delay_in_min, is_canceled
  5. Writes results to S3 Silver as Apache Iceberg table
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import col, lit


# ── Init ─────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "bronze_bucket", "silver_bucket"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["bronze_bucket"]
SILVER_BUCKET = args["silver_bucket"]
REQUIRED_FIELDS = {
    "station", "train_name", "planned_departure",
    "actual_departure", "delay_in_min", "is_canceled"
}


def parse_xml(xml_content: str, s3_path: str) -> list[dict]:
    """Parse raw DB timetable XML into a list of records."""
    records = []
    try:
        root = ET.fromstring(xml_content)
        station = root.attrib.get("station", "")

        for stop in root.findall("s"):
            dp = stop.find("dp")
            ar = stop.find("ar")
            event = dp if dp is not None else ar
            if event is None:
                continue

            record = {
                "station": station,
                "train_name": stop.attrib.get("id", ""),
                "planned_departure": event.attrib.get("pt", ""),
                "actual_departure": event.attrib.get("ct", ""),
                "delay_in_min": event.attrib.get("dl", ""),
                "is_canceled": event.attrib.get("cs", ""),
                "s3_source": s3_path,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            records.append(record)
    except ET.ParseError as e:
        print(f"Failed to parse XML from {s3_path}: {e}")
    return records


def score_quality(records: list[dict]) -> float:
    """quality_score = complete_records / total_records × 100"""
    if not records:
        return 0.0
    complete = sum(
        1 for r in records
        if all(r.get(f) for f in REQUIRED_FIELDS)
    )
    return round(complete / len(records) * 100, 2)


def main():
    # Read all XML files from Bronze
    bronze_path = f"s3://{BRONZE_BUCKET}/bronze/"
    xml_files = (
        sc.wholeTextFiles(bronze_path + "**/*.xml")
        .collect()
    )

    print(f"Found {len(xml_files)} XML files in Bronze")

    all_rows = []
    for s3_path, xml_content in xml_files:
        records = parse_xml(xml_content, s3_path)
        quality_score = score_quality(records)

        # Extract partition info from path
        parts = s3_path.split("/")
        batch_meta = {p.split("=")[0]: p.split("=")[1]
                      for p in parts if "=" in p}

        for record in records:
            record["quality_score"] = quality_score
            record["is_complete"] = all(
                record.get(f) for f in REQUIRED_FIELDS
            )
            record["batch_station"] = batch_meta.get("station", "")
            record["batch_year"] = batch_meta.get("year", "")
            record["batch_month"] = batch_meta.get("month", "")
            record["batch_day"] = batch_meta.get("day", "")
            record["batch_hour"] = batch_meta.get("hour", "")
            all_rows.append(Row(**record))

    if not all_rows:
        print("No records found. Exiting.")
        job.commit()
        return

    # Write to Silver as Iceberg
    df = spark.createDataFrame(all_rows)
    silver_path = f"s3://{SILVER_BUCKET}/silver/train_quality"

    df.write \
        .format("iceberg") \
        .mode("append") \
        .save(silver_path)

    print(f"Wrote {len(all_rows)} records to Silver Iceberg table")
    print(f"Quality scores range: {df.agg({'quality_score': 'min'}).collect()[0][0]} - "
          f"{df.agg({'quality_score': 'max'}).collect()[0][0]}")

    job.commit()


if __name__ == "__main__":
    main()