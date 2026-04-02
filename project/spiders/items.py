import scrapy

class WRCRecord(scrapy.Item):
    record_id = scrapy.Field()
    identifier = scrapy.Field()
    published_date = scrapy.Field()
    description = scrapy.Field()
    body = scrapy.Field()
    body_name = scrapy.Field()
    link_to_doc = scrapy.Field()
    partition_date = scrapy.Field()
    file_path = scrapy.Field()
    file_hash = scrapy.Field()
    file_type = scrapy.Field()
    source_url = scrapy.Field()
    file_content = scrapy.Field()
    source= scrapy.Field()
