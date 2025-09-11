# Data service
It stores crawled data into a DB

## AWS
### AWS login

- Check available profiles
    ```shell
    aws configure list-profiles
    ```

- Login using SSO
    ```shell
    aws sso login --profile "your profile name"
    ```

    ```shell
    # Set up default profile so that you can omit --profile for aws cli commands
    export AWS_PROFILE="your profile"
    ```

## Import data from json file to dynamoDB
```shell
pipenv run python ./data_service/dynamodb_property_service.py | tee "output_$(date +%Y-%m-%d_%H-%M-%S).log"
```
