import os
from minio import Minio
from typing import Iterable
from bs4 import BeautifulSoup
from pymongo import MongoClient

def connect_mongo() -> MongoClient:
    """Create a MongoDB client using environment configuration."""
    return MongoClient(os.getenv("MONGO_URI"))

def get_database(mongo: MongoClient):
    """Return the configured MongoDB database."""
    return mongo[os.getenv("MONGO_DB")]

def get_collections(db):
    """
    Return the configured landing and curated metadata collections.
    Keeping this in base makes future source transforms reuse the same setup.
    """
    source = db[os.getenv("MONGO_COLLECTION")]
    target = db[os.getenv("MONGO_CURATED_COLLECTION")]
    target.create_index(
        [("record_id", 1), ("file_hash", 1)],
        unique=True,
        name="curated_record_version_unique",
    )
    return source, target

def connect_minio() -> Minio:
    """Create a MinIO client using environment configuration."""
    return Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )

def get_buckets(minio: Minio) -> tuple[str, str]:
    """
    Return the configured landing and curated buckets and ensure curated exists.
    """
    landing_bucket = os.getenv("MINIO_BUCKET")
    curated_bucket = os.getenv("MINIO_CURATED_BUCKET")
    ensure_bucket(minio, curated_bucket)
    return landing_bucket, curated_bucket

def fetch_mongo(collection,query) -> Iterable[dict]:
    return collection.find(query)

def ensure_bucket(minio: Minio, bucket: str):
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)

def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)

def clean_html(raw: bytes) -> bytes:
    soup = BeautifulSoup(raw, "lxml")

    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    main = soup.find("main")
    content = main if main else soup.body
    if content is None:
        content = soup

    return str(content).encode("utf-8")