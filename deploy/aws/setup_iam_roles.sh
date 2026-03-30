#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
S3_BUCKET_NAME="myhousetracker-99ce79"
DYNAMODB_TABLE_NAME="properties"

# Role names
TASK_EXECUTION_ROLE_NAME="housetracker-ecs-task-execution-role"
TASK_ROLE_NAME="housetracker-ecs-task-role"

# Permission boundary - caps max permissions any housetracker role can have
PERMISSION_BOUNDARY_ARN="arn:aws:iam::aws:policy/PowerUserAccess"

# Function to create role with permission boundary if not exists
create_role_if_not_exists() {
    local role_name=$1
    local assume_role_policy=$2

    if aws iam get-role --role-name "${role_name}" &> /dev/null; then
        echo "Role already exists: ${role_name}"
    else
        echo "Creating role: ${role_name}"
        aws iam create-role \
            --role-name "${role_name}" \
            --assume-role-policy-document "${assume_role_policy}" \
            --permissions-boundary "${PERMISSION_BOUNDARY_ARN}" \
            --output table
    fi
}

# Function to create role WITHOUT permission boundary if not exists
create_role_without_boundary_if_not_exists() {
    local role_name=$1
    local assume_role_policy=$2

    if aws iam get-role --role-name "${role_name}" &> /dev/null; then
        echo "Role already exists: ${role_name}"
    else
        echo "Creating role: ${role_name}"
        aws iam create-role \
            --role-name "${role_name}" \
            --assume-role-policy-document "${assume_role_policy}" \
            --output table
    fi
}

# Function to attach policy if not already attached
attach_policy_if_not_attached() {
    local role_name=$1
    local policy_arn=$2

    if aws iam list-attached-role-policies --role-name "${role_name}" \
        --query "AttachedPolicies[?PolicyArn=='${policy_arn}']" \
        --output text | grep -q "${policy_arn}"; then
        echo "Policy already attached: ${policy_arn}"
    else
        echo "Attaching policy: ${policy_arn}"
        aws iam attach-role-policy \
            --role-name "${role_name}" \
            --policy-arn "${policy_arn}"
    fi
}

# Trust policy: allows ECS tasks to assume this role
ECS_TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ecs-tasks.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "=========================================="
echo "Step 1: Create Task Execution Role"
echo "=========================================="
echo "Purpose: Allows ECS agent to pull images from ECR and write logs to CloudWatch"
echo ""

create_role_if_not_exists \
    "${TASK_EXECUTION_ROLE_NAME}" \
    "${ECS_TRUST_POLICY}"

# Attach AWS managed policy for task execution (ECR pull + CloudWatch logs)
attach_policy_if_not_attached \
    "${TASK_EXECUTION_ROLE_NAME}" \
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"

echo ""
echo "=========================================="
echo "Step 2: Create Task Role"
echo "=========================================="
echo "Purpose: Allows containers to access DynamoDB and S3"
echo ""

create_role_if_not_exists \
    "${TASK_ROLE_NAME}" \
    "${ECS_TRUST_POLICY}"

# Create inline policy for DynamoDB and S3 access
TASK_POLICY="{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Sid\": \"DynamoDBAccess\",
            \"Effect\": \"Allow\",
            \"Action\": [
                \"dynamodb:GetItem\",
                \"dynamodb:PutItem\",
                \"dynamodb:UpdateItem\",
                \"dynamodb:DeleteItem\",
                \"dynamodb:Query\",
                \"dynamodb:Scan\",
                \"dynamodb:DescribeTable\",
                \"dynamodb:BatchWriteItem\",
                \"dynamodb:BatchGetItem\"
            ],
            \"Resource\": [
                \"arn:aws:dynamodb:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/*\",
                \"arn:aws:dynamodb:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/*/index/*\"
            ]
        },
        {
            \"Sid\": \"DynamoDBListTables\",
            \"Effect\": \"Allow\",
            \"Action\": \"dynamodb:ListTables\",
            \"Resource\": \"*\"
        },
        {
            \"Sid\": \"S3Access\",
            \"Effect\": \"Allow\",
            \"Action\": [
                \"s3:PutObject\",
                \"s3:GetObject\",
                \"s3:ListBucket\",
                \"s3:HeadBucket\",
                \"s3:HeadObject\"
            ],
            \"Resource\": [
                \"arn:aws:s3:::${S3_BUCKET_NAME}\",
                \"arn:aws:s3:::${S3_BUCKET_NAME}/*\"
            ]
        }
    ]
}"

# Put inline policy (idempotent - overwrites if exists)
echo "Attaching DynamoDB and S3 policy to task role..."
aws iam put-role-policy \
    --role-name "${TASK_ROLE_NAME}" \
    --policy-name "housetracker-task-policy" \
    --policy-document "${TASK_POLICY}"

echo ""
echo "=========================================="
echo "Step 3: Create EventBridge Scheduler Role"
echo "=========================================="
echo "Purpose: Allows EventBridge Scheduler to trigger ECS tasks"
echo ""

EVENTBRIDGE_ROLE_NAME="housetracker-eventbridge-ecs-role"

EVENTBRIDGE_TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "scheduler.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}'

create_role_without_boundary_if_not_exists \
    "${EVENTBRIDGE_ROLE_NAME}" \
    "${EVENTBRIDGE_TRUST_POLICY}"

EVENTBRIDGE_POLICY="{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Sid\": \"RunEcsTask\",
            \"Effect\": \"Allow\",
            \"Action\": \"ecs:RunTask\",
            \"Resource\": \"arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/*\"
        },
        {
            \"Sid\": \"PassRoleToEcs\",
            \"Effect\": \"Allow\",
            \"Action\": \"iam:PassRole\",
            \"Resource\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/*\",
            \"Condition\": {
                \"StringEquals\": {
                    \"iam:PassedToService\": \"ecs-tasks.amazonaws.com\"
                }
            }
        }
    ]
}"

echo "Attaching policy to EventBridge role..."
aws iam put-role-policy \
    --role-name "${EVENTBRIDGE_ROLE_NAME}" \
    --policy-name "eventbridge-run-ecs-task" \
    --policy-document "${EVENTBRIDGE_POLICY}"

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
EXECUTION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${TASK_EXECUTION_ROLE_NAME}"
TASK_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${TASK_ROLE_NAME}"
EVENTBRIDGE_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${EVENTBRIDGE_ROLE_NAME}"
echo "  TASK_EXECUTION_ROLE_ARN=${EXECUTION_ROLE_ARN}"
echo "  TASK_ROLE_ARN=${TASK_ROLE_ARN}"
echo "  EVENTBRIDGE_ROLE_ARN=${EVENTBRIDGE_ROLE_ARN}"
