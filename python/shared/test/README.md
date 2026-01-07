# Unit test for "shared" package

## Run Redis stream tests
0. Prerequisites: set up python path: [link](../../README.md#prerequisite---before-running-any-python-files)

1. Start a local Redis server. Local Redis config is at python/shared/test/config/redis_test.config.json

    You can run it using docker compose file: python/docker-compose.dev.yml
    ```shell
    docker compose -f docker-compose.dev.yml up
    ```

2. Run tests
    ```shell
    # Run all tests
    pipenv run python shared/test/redis_stream_test.py
    ```

    ```shell
    # Run a subset of tests
    pipenv run python shared/test/redis_stream_test.py "test class name"

    # Example:
    pipenv run python shared/test/redis_stream_test.py TestRedisStreamIntegration
    ```

