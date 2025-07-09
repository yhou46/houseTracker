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
