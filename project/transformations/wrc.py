import io
import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv

from project.spiders.utils.helpers import clean_text, normalize_date_string, sha256_bytes

from .base import (
    connect_minio,
    connect_mongo,
    fetch_mongo,
    get_buckets,
    get_collections,
    get_database,
    safe_filename,
    clean_html
)

logger = logging.getLogger(__name__)

def transform(start_date: str, end_date: str):
    load_dotenv()

    mongo = connect_mongo()
    db = get_database(mongo)
    source, target = get_collections(db)

    minio = connect_minio()
    landing_bucket, curated_bucket = get_buckets(minio)

    processed = 0
    skipped = 0

    for record in fetch_mongo(source, query = {"partition_date": {"$gte": start_date, "$lt": end_date}}):
        identifier = record.get("identifier") or "unknown"
        file_type = record.get("file_type") or "bin"
        file_path = record.get("file_path")

        if not file_path:
            logger.info(
                json.dumps(
                    {
                        "event": "transform_skipped",
                        "record_id": record.get("record_id"),
                        "reason": "missing_file_path",
                    }
                )
            )
            continue

        obj = minio.get_object(landing_bucket, file_path)
        try:
            raw = obj.read()
        finally:
            obj.close()
            obj.release_conn()

        transformed = clean_html(raw) if file_type == "html" else raw
        curated_hash = sha256_bytes(transformed)

        existing = target.find_one(
            {"record_id": record.get("record_id"), "file_hash": curated_hash}
        )
        if existing:
            skipped += 1
            logger.info(
                json.dumps(
                    {
                        "event": "transform_skipped",
                        "record_id": record.get("record_id"),
                        "identifier": identifier,
                        "reason": "unchanged_curated_version",
                    }
                )
            )
            continue

        safe_identifier = safe_filename(identifier)
        curated_path = (
            f'{record.get("partition_date", "unknown")}/'
            f'{record.get("record_id", "unknown")}/'
            f'{safe_identifier}.{file_type}'
        )

        minio.put_object(
            curated_bucket,
            curated_path,
            io.BytesIO(transformed),
            length=len(transformed),
        )

        curated_metadata = dict(record)
        curated_metadata["identifier"] = clean_text(record.get("identifier"))
        curated_metadata["description"] = clean_text(record.get("description"))
        curated_metadata["published_date"] = normalize_date_string(
            record.get("published_date"),
            input_format="%d/%m/%Y",
            output_format="%Y-%m-%d",
            logger=logger,
            log_event="invalid_published_date_format",
        )
        curated_metadata["file_path"] = curated_path
        curated_metadata["file_hash"] = curated_hash
        curated_metadata["updated_at"] = datetime.utcnow()
        curated_metadata.pop("_id", None)

        target.update_one(
            {"record_id": record.get("record_id"), "file_hash": curated_hash},
            {
                "$setOnInsert": {
                    **curated_metadata,
                    "created_at": curated_metadata["updated_at"],
                }
            },
            upsert=True,
        )
        processed += 1

        logger.info(
            json.dumps(
                {
                    "event": "transform_saved",
                    "record_id": record.get("record_id"),
                    "identifier": identifier,
                    "partition_date": record.get("partition_date"),
                    "file_path": curated_path,
                    "file_type": file_type,
                }
            )
        )

    logger.info(
        json.dumps(
            {
                "event": "transform_summary",
                "start_date": start_date,
                "end_date": end_date,
                "processed": processed,
                "skipped": skipped,
            }
        )
    )

    mongo.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Transform WRC landing-zone files into curated-zone files."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(message)s")
    transform(args.start_date, args.end_date)
