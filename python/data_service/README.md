# Data service
It stores crawled data into a DB

TODO:
- Need have a service to scan active or pending properties in DB and crawll again to update the status since the crawler doesn't crawl off market or sold properties by default
- Complete query function in dynamoDB service

test1

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

## Property query tools
```shell
pipenv run python data_service/property_query_tool.py --id "<property id>"
pipenv run python data_service/property_query_tool.py --address "<full address string>"
```
