# Scrapy settings for wired project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from pika.exchange_type import ExchangeType

BOT_NAME = "wired"

SPIDER_MODULES = ["wired.spiders"]
NEWSPIDER_MODULE = "wired.spiders"
CLOSESPIDER_ITEMCOUNT = 500
ROBOTSTXT_OBEY = True
# RABBITMQ
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_EXCHANGE = "news_subject"
RABBITMQ_EXCHANGE_TYPE = ExchangeType.direct
RABBITMQ_QUEUES = ["Science", "Business", "Culture", "Security"]
RABBITMQ_ROUTING_KEYS = ["science", "business", "culture", "security"]

ITEM_PIPELINES = {
    "wired.pipelines.PikaProducer": 200,
}
LOG_LEVEL = "INFO"
