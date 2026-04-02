import os
from importlib import import_module
from dotenv import load_dotenv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.spiderloader import SpiderLoader

load_dotenv()


def get_spider_class(spider_name: str):
    """
    Resolve a Scrapy spider class from its registered spider name.
    """
    settings = get_project_settings()
    loader = SpiderLoader.from_settings(settings)
    return loader.load(spider_name)


def run(start_date: str, end_date: str, spider_name: str | None = None):
    """Run a spider with explicit date inputs."""
    settings = get_project_settings()
    spider_name = spider_name or os.getenv("SPIDER_NAME", "wrc")
    spider_class = get_spider_class(spider_name)
    process = CrawlerProcess(settings)

    process.crawl(
        spider_class,
        start_date=start_date,
        end_date=end_date,
    )

    process.start()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run a configured scraper.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--spider-name",
        default=os.getenv("SPIDER_NAME", "wrc"),
        help="Registered Scrapy spider name, for example 'wrc'",
    )
    args = parser.parse_args()
    run(args.start_date, args.end_date, spider_name=args.spider_name)
