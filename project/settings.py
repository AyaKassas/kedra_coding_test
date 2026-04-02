import os
from dotenv import load_dotenv

# Load .env at project root when settings are imported.
load_dotenv()

BOT_NAME = "wrc_scraper"

# Where Scrapy looks for spiders.
SPIDER_MODULES = ["project.spiders"]
NEWSPIDER_MODULE = "project.spiders"

# Politeness + performance tuning. All can be overridden via env vars.
ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "16"))
CONCURRENT_REQUESTS_PER_DOMAIN = int(os.getenv("CONCURRENT_REQUESTS_PER_DOMAIN", "8"))
DOWNLOAD_DELAY = float(os.getenv("DOWNLOAD_DELAY", "0.25"))
RANDOMIZE_DOWNLOAD_DELAY = True

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = float(os.getenv("AUTOTHROTTLE_START_DELAY", "0.25"))
AUTOTHROTTLE_MAX_DELAY = float(os.getenv("AUTOTHROTTLE_MAX_DELAY", "5"))

RETRY_ENABLED = True
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "3"))
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(message)s"

# Scrape configuration
SPIDER_NAME = os.getenv("SPIDER_NAME", "wrc")
SCRAPE_BODIES = os.getenv("SCRAPE_BODIES", "2,1,3,15376")
PARTITION_SIZE = os.getenv("PARTITION_SIZE", "monthly")

# Storage configuration (Mongo + MinIO)
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
MONGO_CURATED_COLLECTION = os.getenv("MONGO_CURATED_COLLECTION")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_CURATED_BUCKET = os.getenv("MINIO_CURATED_BUCKET")

# Middlewares for headers and basic anti-blocking.
DOWNLOADER_MIDDLEWARES = {
    "project.middlewares.UserAgentMiddleware": 400,
}

# Gives priority to StoragePipeline, which saves items to MongoDB and MinIO.
# Lower is higher priority.
ITEM_PIPELINES = {
    "project.pipelines.StoragePipeline": 300,
}
