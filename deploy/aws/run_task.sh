#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$0")"
CONFIG_FILE="${SCRIPT_DIR}/config/ecs_run_task_config.json"

# Load config
AWS_REGION=$(jq -r '.aws.region' "${CONFIG_FILE}")
CLUSTER_NAME=$(jq -r '.ecs.cluster_name' "${CONFIG_FILE}")
TASK_FAMILY=$(jq -r '.ecs.task_family' "${CONFIG_FILE}")
SECURITY_GROUP_NAME=$(jq -r '.networking.security_group_name' "${CONFIG_FILE}")

echo "=========================================="
echo "Run ECS Task Manually"
echo "=========================================="
echo ""

# Look up AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Look up VPC
DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
    --region "${AWS_REGION}" \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text)

# Look up subnets
SUBNET_IDS=$(aws ec2 describe-subnets \
    --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "Subnets[*].SubnetId" \
    --output text)

# Use first subnet only
SUBNET_ID=$(echo "${SUBNET_IDS}" | awk '{print $1}')

# Look up security group
SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
    --region "${AWS_REGION}" \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "SecurityGroups[0].GroupId" \
    --output text)

echo "Cluster:        ${CLUSTER_NAME}"
echo "Task family:    ${TASK_FAMILY}"
echo "Subnet:         ${SUBNET_ID}"
echo "Security group: ${SECURITY_GROUP_ID}"
echo ""

TASK_ARN=$(aws ecs run-task \
    --region "${AWS_REGION}" \
    --cluster "${CLUSTER_NAME}" \
    --task-definition "${TASK_FAMILY}" \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={
        subnets=[${SUBNET_ID}],
        securityGroups=[${SECURITY_GROUP_ID}],
        assignPublicIp=ENABLED
    }" \
    --query "tasks[0].taskArn" \
    --output text)

echo "Task started: ${TASK_ARN}"
echo ""
echo "View logs:"
echo "  https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#logsV2:log-groups/log-group/%2Fecs%2Fhousetracker"
echo ""
echo "View task status:"
echo "  aws ecs describe-tasks --cluster ${CLUSTER_NAME} --tasks ${TASK_ARN} --region ${AWS_REGION}"
