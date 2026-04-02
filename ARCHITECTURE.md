## Architecture Summary

### Partition Strategy
The default partition size is `monthly`. The source website already supports start and end date filters, so monthly windows keep the run fast while still being small enough to retry safely. If a month ever becomes too large, the same code can switch to `weekly` or `daily` through `PARTITION_SIZE` without code changes.

### Rate Limiting And Retries
The scraper uses Scrapy concurrency, a small download delay, AutoThrottle, and retries for `429` and common `5xx` responses. A lightweight rotating user-agent middleware is also enabled. This keeps the scraper reasonably fast while lowering the chance of getting blocked. Every partition start, partition summary, failed download, skipped unchanged file, and transform summary is logged as JSON so failures are easy to trace.

### Deduplication And Idempotency
Each logical source record gets a stable `record_id`, and each stored file gets a `file_hash`. In MongoDB, the landing-zone collection keeps one document per unique `record_id + file_hash` pair. In MinIO, landing files are stored under a hash-based path. This gives us append-only storage for the landing zone:

- re-running the same date range does not create duplicates
- unchanged files are skipped
- changed files create a new version instead of overwriting old landing data

The curated zone follows the same metadata versioning rule, while the actual file name is still written as `identifier.ext` as requested.

### Transformation
The transformation script reads landing metadata from MongoDB for the requested date range, fetches the related files from MinIO, and only transforms HTML files. HTML is cleaned with BeautifulSoup then re-hashed and stored in the curated bucket. PDF and Word files pass through unchanged.

### If This Needed 50+ Sources
I would keep the same overall pattern but do the following:

1. Per-source spiders with isolated selectors and source-specific throttling.
    - each site has different HTML and different rate limits
    - concurrency/delay can be tuned per source if one site is more sensitive
2. A richer Dagster setup with schedules, backfills, and alerts per source.
    - schedule each source independently
    - rerun failed date ranges for one source without touching others
    - add alerts when a source fails or suddenly returns too few records
