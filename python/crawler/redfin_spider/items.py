# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class RedfinPropertyItem(scrapy.Item):
    # Basic property information
    address = scrapy.Field()
    area = scrapy.Field()  # Square footage
    propertyType = scrapy.Field()
    lotArea = scrapy.Field()
    numberOfBedroom = scrapy.Field()
    numberOfBathroom = scrapy.Field()
    yearBuilt = scrapy.Field()
    status = scrapy.Field()
    history = scrapy.Field()
    historyCount = scrapy.Field()
    
    # Additional metadata
    url = scrapy.Field()
    redfinId = scrapy.Field()
    scrapedAt = scrapy.Field()
    spiderName = scrapy.Field()
