# Run the crawler

## Redfin crawler
- Prerequisite
From repoRoot/python
```shell
pipenv shell
```

- Run with debug
```shell
cd python/crawler
scrapy crawl redfin_spider_monolith-L DEBUG
```

- Run with only a few items
```shell
cd python/crawler
python -m scrapy crawl redfin_spider_monolith -s CLOSESPIDER_ITEMCOUNT=1
```

## Pipelines

### AWS S3
It uploads data to S3 based on zip code.

## Run test
1. Need to set up PYTHONPATH
    ```shell
    cd python/crawler
    export PYTHONPATH="$(pwd):$PYTHONPATH"
    ```

2. From repoRoot/python/crawler
    ```shell
    pipenv run python redfin_spider/test_parser.py
    ```