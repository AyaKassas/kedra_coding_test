import io
import json
from datetime import datetime
from pymongo import MongoClient
from minio import Minio

from project.spiders.utils.helpers import sha256_bytes
from project.transformations.base import clean_html


class StoragePipeline:
    """
    Scrapy sends each item to pipelines.py.
    Stores metadata in MongoDB and raw files in MinIO (Landing Zone).
    Idempotent:
    - unchanged files are skipped
    - changed files create a new version instead of overwriting landing data
    """

    def open_spider(self, spider):
        settings = spider.settings

        self.mongo = MongoClient(settings.get("MONGO_URI"))
        self.collection = self.mongo[settings.get("MONGO_DB")][
            settings.get("MONGO_COLLECTION")
        ]
        self.collection.create_index(
            [("record_id", 1), ("file_hash", 1)],
            unique=True,
            name="record_version_unique",
        )

        self.minio = Minio(
            settings.get("MINIO_ENDPOINT"),
            access_key=settings.get("MINIO_ACCESS_KEY"),
            secret_key=settings.get("MINIO_SECRET_KEY"),
            secure=False,
        )
        self.bucket = settings.get("MINIO_BUCKET")
        self._ensure_bucket(self.bucket)

    def close_spider(self, spider):
        self.mongo.close()

    def process_item(self, item, spider):
        content = item.get("file_content") or b""
        file_type = item.get("file_type") or "bin"

        hash_content = clean_html(content) if file_type == "html" else content
        file_hash = sha256_bytes(hash_content)
        item["file_hash"] = file_hash

        existing = self.collection.find_one(
            {"record_id": item["record_id"], "file_hash": file_hash}
        )
        if existing:
            spider.logger.info(
                json.dumps(
                    {
                        "event": "skipped_unchanged",
                        "record_id": item["record_id"],
                        "identifier": item["identifier"],
                        "partition_date": item["partition_date"],
                    }
                )
            )
            item["file_path"] = existing.get("file_path")
            return item

        ext = file_type
        safe_identifier = self._safe_filename(item["identifier"] or "unknown")
        path = f'{item["partition_date"]}/{safe_identifier}/{file_hash}.{ext}'
        item["file_path"] = path

        self.minio.put_object(
            self.bucket,
            path,
            io.BytesIO(content),
            length=len(content),
        )

        metadata = dict(item)
        metadata.pop("file_content", None)
        metadata["updated_at"] = datetime.utcnow()

        self.collection.update_one(
            {"record_id": item["record_id"], "file_hash": file_hash},
            {
                "$setOnInsert": {
                    **metadata,
                    "created_at": metadata["updated_at"],
                }
            },
            upsert=True,
        )

        return item

    def _ensure_bucket(self, bucket):
        try:
            if not self.minio.bucket_exists(bucket):
                self.minio.make_bucket(bucket)
        except Exception as exc:
            raise RuntimeError(
                "Could not connect to MinIO using the configured MINIO_ENDPOINT. "
                "Make sure Docker is running, the MinIO container is healthy, "
                "and the API port matches .env (usually localhost:9000)."
            ) from exc

    def _safe_filename(self, name):
        return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)
