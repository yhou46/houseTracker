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
scrapy crawl redfin_spider -L DEBUG
```

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