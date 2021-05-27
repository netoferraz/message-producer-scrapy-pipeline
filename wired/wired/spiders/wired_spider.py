from scrapy.spiders import SitemapSpider
from scrapy.loader import ItemLoader
from wired.items import NewsItem
import logging


class WiredSpider(SitemapSpider):
    name = "newswired"
    sitemap_urls = ["https://www.wired.co.uk/sitemap.xml"]
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    def parse(self, response):
        l = ItemLoader(item=NewsItem(), response=response)
        l.add_value(
            "category",
            response.xpath(
                "//main[@id='main-content']//a[contains(@href, '/topic/')]"
            ).get(),
        )
        l.add_value("url", response.url)
        return l.load_item()
