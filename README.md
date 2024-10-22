# To run the script
1. Use pipenv shell
    ```shell
    pipenv shell
    ```

2. Then run the script as usual

3. To quit from pipenv shell, just type "exit"

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
