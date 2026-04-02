import os

from dotenv import load_dotenv
from minio import Minio
from pymongo import MongoClient


def main():
    load_dotenv()

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    print("Checking services...\n")

    mongo_ok = False
    minio_ok = False

    try:
        mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo.admin.command("ping")
        print(f"MongoDB: OK ({mongo_uri})")
        if mongo_db:
            print(f"MongoDB database configured: {mongo_db}")
        mongo_ok = True
        mongo.close()
    except Exception as exc:
        print(f"MongoDB: FAILED ({mongo_uri})")
        print(f"Reason: {exc}")

    print()

    try:
        minio = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        buckets = minio.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]
        print(f"MinIO: OK ({minio_endpoint})")
        print(f"Buckets: {bucket_names}")
        minio_ok = True
    except Exception as exc:
        print(f"MinIO: FAILED ({minio_endpoint})")
        print(f"Reason: {exc}")
        print("Tip: open http://localhost:19001 in your browser and confirm the MinIO console loads.")
        print("Tip: if it does not load, run `docker compose up -d` first.")

    print()

    if mongo_ok and minio_ok:
        print("All required services are reachable.")
    else:
        print("One or more services are not ready.")
        print("Fix the failed service before running the scraper or transform.")


if __name__ == "__main__":
    main()
