import scrapy

from .utils.logger import LoggingSpider
from .items import WRCRecord
from .utils.helpers import (
    build_record_id,
    date_partitions,
    infer_content_type,
)

class WRCSpider(LoggingSpider):
    name = "wrc"
    allowed_domains = ["workplacerelations.ie"]
    base_url = "https://www.workplacerelations.ie/en/search/?decisions=1&"
    body_names = {
        "2": "Employment Appeals Tribunal",
        "1": "Equality Tribunal",
        "3": "Labour Court",
        "15376": "Workplace Relations Commission",
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        Scrapy factory method.

        This is called AFTER Scrapy creates the spider and attaches
        settings + crawler context.

        Use this when you need access to crawler.settings.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)

        bodies_raw = crawler.settings.get("SCRAPE_BODIES", "2,1,3,15376")
        spider.bodies = [
            body.strip()
            for body in bodies_raw.split(",")
            if body.strip()
        ]

        return spider

    def __init__(self, start_date, end_date, *args, **kwargs):
        """
        Spider constructor.

        Called when you run:
        scrapy crawl wrc -a start_date=... -a end_date=...
        """
        super().__init__(*args, **kwargs)

        self.start_date = start_date
        self.end_date = end_date
        self.bodies = []

    def start_requests(self):
        partition_size = self.settings.get("PARTITION_SIZE", "monthly")
        for start, end in date_partitions(self.start_date, self.end_date, partition_size):
            for body in self.bodies:
                url = (
                    f"{self.base_url}"
                    f"&from={start}&to={end}"
                    f"&body={body}"
                    f"&pageNumber=1"
                )
                self.log_partition_start(start, body, url)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    errback=self.handle_request_failure,
                    cb_kwargs={"partition_date": start, "body": body},
                )

    async def start(self):
        for request in self.start_requests():
            yield request

    def parse(self, response, partition_date, body):
        self.ensure_stats(partition_date, body)

        for item in response.css("li.each-item"):
            identifier = item.css("h2.title a::text").get() or ""
            published_date = item.css("span.date::text").get() or ""
            description = item.css("p.description::text").get() or ""
            link_to_doc = item.css("div.link a::attr(href)").get()

            if not link_to_doc:
                self.log_failed_download(
                    partition_date=partition_date,
                    body=body,
                    url=response.url,
                    status=response.status,
                    reason="missing_document_link",
                )
                continue

            self.increment_found(partition_date, body)

            yield response.follow(
                url=link_to_doc,
                callback=self.parse_document,
                errback=self.handle_request_failure,
                cb_kwargs={
                    "identifier": identifier,
                    "published_date": published_date,
                    "description": description,
                    "result_url": response.urljoin(link_to_doc),
                    "partition_date": partition_date,
                    "body": body,
                },
            )

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                errback=self.handle_request_failure,
                cb_kwargs={"partition_date": partition_date, "body": body},
            )

    def parse_document(
        self,
        response,
        identifier,
        published_date,
        description,
        result_url,
        partition_date,
        body,
    ):
        self.ensure_stats(partition_date, body)

        if response.status != 200:
            self.log_failed_download(
                partition_date=partition_date,
                body=body,
                url=response.url,
                status=response.status,
                reason="non_200_response",
            )
            return

        header = response.headers.get(b"Content-Type", b"").decode("utf-8")
        content_type = infer_content_type(response.url, header)
        content = response.body
        record_id = build_record_id(identifier, result_url, published_date, body)

        self.increment_scraped(partition_date, body)

        yield WRCRecord(
            record_id=record_id,
            identifier=identifier,
            published_date=published_date,
            description=description,
            link_to_doc=response.url,
            partition_date=partition_date,
            source_url=result_url,
            body=body,
            body_name=self.body_names.get(body, body),
            file_type=content_type,
            file_content=content,
            source="wrc"
)
