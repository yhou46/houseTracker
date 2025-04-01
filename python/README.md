All commands should be executed from python folder under repo root directory.
```shell
cd python
```

# TODO:
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
```shell
export PYTHONPATH=<path to repo>/houseTracker/python:$PYTHONPATH
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
