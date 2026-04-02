import os
from importlib import import_module
from dagster import job, op
from dotenv import load_dotenv

from run_spider import run


load_dotenv()

def get_transform_function(spider_name: str):
    """
    Load the matching transformation module for the selected spider.
    Example: spider 'wrc' -> project.transformations.wrc.transform
    """
    module = import_module(f"project.transformations.{spider_name}")
    return module.transform

@op
def ingest_op():
    """
    Step 1: scrape source data into the landing zone.
    """
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")
    spider_name = os.getenv("SPIDER_NAME", "wrc")
    run(start_date, end_date, spider_name=spider_name)
    return "ingest_finished"

@op
def transform_op(start_after):
    """
    Step 2: transform landing-zone files into curated-zone files.
    """
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")
    spider_name = os.getenv("SPIDER_NAME", "wrc")
    transform = get_transform_function(spider_name)
    transform(start_date, end_date)


@job
def ingestion_pipeline():
    # Dagster handles the dependency so transform only starts after ingest ends.
    transform_op(ingest_op())
