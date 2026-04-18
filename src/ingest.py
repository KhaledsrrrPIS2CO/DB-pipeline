"""
ingest.py — DB Timetables API → S3 Bronze

What this does:
  1. Calls DB Timetables API for Hamburg Hbf, current date/hour
  2. Dumps raw XML to S3 Bronze, partitioned by date/hour
  3. Logs what happened

Raw XML is stored as-is. No transformation. No parsing.
Bronze = source of truth. Immutable. Never touched again after landing.
"""

import os
import logging
from datetime import datetime, timezone

import boto3
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_CLIENT_ID  = os.environ["DB_CLIENT_ID"]
DB_API_KEY    = os.environ["DB_API_KEY"]
BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]

STATION_EVA  = "8002549"
STATION_NAME = "Hamburg-Hbf"
DB_API_BASE  = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"


def fetch_timetable(eva: str, date: str, hour: str) -> bytes:
    url = f"{DB_API_BASE}/plan/{eva}/{date}/{hour}"
    headers = {
        "DB-Client-ID": DB_CLIENT_ID,
        "DB-Api-Key":   DB_API_KEY,
        "Accept":       "application/xml",
    }
    log.info(f"Calling DB API: {url}")
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    log.info(f"API response: {response.status_code}, {len(response.content)} bytes")
    return response.content


def s3_key(station: str, date: str, hour: str) -> str:
    year  = f"20{date[:2]}"
    month = date[2:4]
    day   = date[4:6]
    return (
        f"bronze/"
        f"station={station}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"timetable.xml"
    )


def upload_to_s3(bucket: str, key: str, data: bytes) -> None:
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/xml",
    )
    log.info(f"Uploaded to s3://{bucket}/{key}")


def main() -> None:
    now  = datetime.now(timezone.utc)
    date = now.strftime("%y%m%d")
    hour = now.strftime("%H")
    log.info(f"Ingesting timetable — station={STATION_NAME}, date={date}, hour={hour}")
    xml_bytes = fetch_timetable(STATION_EVA, date, hour)
    key = s3_key(STATION_NAME, date, hour)
    upload_to_s3(BRONZE_BUCKET, key, xml_bytes)
    log.info("Done. Raw data is in Bronze. Do not touch it.")


if __name__ == "__main__":
    main()