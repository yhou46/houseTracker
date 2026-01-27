# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PropertyUrlItem(scrapy.Item):
    """
    Item for property URLs discovered from search results.
    Used by PropertyUrlDiscoverySpider to publish URLs to Redis Stream.
    """
    property_url = scrapy.Field()       # Absolute URL to property page
    from_page_url = scrapy.Field()      # Search page URL where discovered
    scraped_at_utc = scrapy.Field()     # ISO formatted UTC timestamp
    data_source = scrapy.Field()        # Data source name (e.g., "Redfin")


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
    price = scrapy.Field()
    readyToBuildTag = scrapy.Field()

    # Additional metadata
    url = scrapy.Field()
    redfinId = scrapy.Field()
    scrapedAt = scrapy.Field()
    spiderName = scrapy.Field()
    zipCode = scrapy.Field()  # Zip code extracted from URL or address
