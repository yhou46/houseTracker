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
