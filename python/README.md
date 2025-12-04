All commands should be executed from python folder under repo root directory.
```shell
cd python
```

# TODO:
- Use scrapy to crawl data
scrapy -> parsed jsonl file -> service parse the jsonl file(convert to iproperty object, add id, check duplicates) and store in DB
- Move iproperty to separate package?
- Create a hash function to hash address
- Add/Create #MLS and parcelNumber (tax) to property
- How to track if property basic propery changed? Like area change and room change? Add last update time?
- Store data into DB (mongodb for now)


- 3 tables:
property metadata
price history, state
government site metadata

# Install required packages
1. Install packages
    ```shell

    pipenv install
    ```

2. Install new packages
    ```shell
    pipenv install <package name>

    # Install dev dependencies
    pipenv install <package name> --dev
    ```

# Prerequisite - before running any python files
```shell
cd houseTracker/python
export PYTHONPATH="$(pwd):$PYTHONPATH"
```

# To run the script
1. Use pipenv shell
    ```shell
    pipenv shell
    ```

2. Run the command in pipenv shell
    ```shell
    pipenv run python <your script>.py
    ```

3. To quit from pipenv shell, just type "exit"

# To run unit test:
1. export current directory to python path:
    Need to edit PYTHONPATH to point to repoRoot/python
    ```shell
    cd repoRoot/python
    export PYTHONPATH="$(pwd):$PYTHONPATH"
    ```

2. Run from repoRoot/python:
```shell
pipenv run python ./shared/test/iproperty.test.py
```

# Compile test
```shell
# please do the prerequisite first
mypy ./
```

# Update python version in pipenv

1. Remove the environment
    ```shell
    pipenv --rm
    ```

2. Update python version in Pipfile
    ```shell
    [requires]
    python_version = "<new version>"
    ```

2. Update the environment
    ```shell
    pipenv update
    ```

# Design
## Data fetch flow
```
StartUrls
    |-> UrlDispatcher
            |-> PropertyUrlQueue
                |-> PropertyCrawler
                    |-> RawPropertyDataQueue
                    |       |-> DbService
                    |               |-> Database
                    |-> RawDataStorage
```

- StartUrls:
Something like https://www.redfin.com/city/16163/WA/Seattle

- PropertyUrlQueue
Message queue for property URL from start URLs, one example property URL: https://www.redfin.com/WA/Seattle/\<address>/home/\<redfin id>. Property URLs can be batched in a single message.

- PropertyCrawler
Request property URL, parse into raw data and store into RawPropertyDataQueue

- DbService
Consume raw data, convert it to more structured data and store data into a DB

- RawDataStorage
Store raw data for record keeping and message playback

## Data update flow for active properties in DB
It is to get in market property and update its state.

```
Database
    |-> ScanService
            |-> PropertyUrlQueue
                    |-> PropertyCrawler
                            |-> RawPropertyDataQueue
                                    |-> DbService
                                            |-> Database
```

- ScanService
Query DB to get list of in market property, request URL and update its status
