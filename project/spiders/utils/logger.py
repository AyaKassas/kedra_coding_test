import json
import scrapy


class LoggingSpider(scrapy.Spider):
    """
    Reusable base spider for partitioned scrapers.

    It centralizes the JSON logging and per-partition counters so future
    scrapers can reuse the same behavior without copying helper methods.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.partition_stats = {}
        self.failed_downloads = []

    def ensure_stats(self, partition_date, body):
        "This guarantees the dictionary exists before updating."
        key = (partition_date, body)
        if key not in self.partition_stats:
            self.partition_stats[key] = {"found": 0, "scraped": 0, "failed": 0}

    def increment_found(self, partition_date, body, count=1):
        self.ensure_stats(partition_date, body)
        self.partition_stats[(partition_date, body)]["found"] += count

    def increment_scraped(self, partition_date, body, count=1):
        self.ensure_stats(partition_date, body)
        self.partition_stats[(partition_date, body)]["scraped"] += count

    def log_failed_download(self, partition_date, body, url, status, reason):
        self.ensure_stats(partition_date, body)
        self.partition_stats[(partition_date, body)]["failed"] += 1
        self.failed_downloads.append(
            {
                "partition_date": partition_date,
                "body": body,
                "url": url,
                "status": status,
                "reason": reason,
            }
        )

    def log_partition_start(self, partition_date, body, url):
        """Logs the start of a partition crawl."""
        self.logger.info(
            json.dumps(
                {
                    "event": "partition_start",
                    "partition_date": partition_date,
                    "body": body,
                    "url": url,
                    "triggered_at": str(self.crawler.stats.get_value("start_time"))
                }
            )
        )

    def handle_request_failure(self, failure):
        """
        Generic Scrapy errback for partitioned spiders.
        It expects partition_date/body to be passed through cb_kwargs.
        """
        request = failure.request
        partition_date = request.cb_kwargs.get("partition_date")
        body = request.cb_kwargs.get("body")
        self.ensure_stats(partition_date, body)

        status = getattr(getattr(failure.value, "response", None), "status", None)
        self.log_failed_download(
            partition_date=partition_date,
            body=body,
            url=request.url,
            status=status,
            reason=str(failure.value),
        )

    def closed(self, reason):
        total_found = 0
        total_scraped = 0
        total_failed = 0

        for (partition_date, body), stats in self.partition_stats.items():
            total_found += stats["found"]
            total_scraped += stats["scraped"]
            total_failed += stats["failed"]
            self.logger.info(
                json.dumps(
                    {
                        "event": "partition_summary",
                        "partition_date": partition_date,
                        "body": body,
                        "found": stats["found"],
                        "scraped": stats["scraped"],
                        "failed": stats["failed"],
                    }
                )
            )

        if self.failed_downloads:
            self.logger.info(
                json.dumps(
                    {
                        "event": "failed_downloads",
                        "count": len(self.failed_downloads),
                        "items": self.failed_downloads,
                    }
                )
            )

        self.logger.info(
            json.dumps(
                {
                    "event": "run_summary",
                    "reason": reason,
                    "found": total_found,
                    "scraped": total_scraped,
                    "failed": total_failed,
                }
            )
        )
