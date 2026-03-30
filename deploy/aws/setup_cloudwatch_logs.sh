#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
LOG_GROUP_NAME="/ecs/housetracker"
RETENTION_DAYS=30

echo "=========================================="
echo "Create CloudWatch Log Group"
echo "=========================================="

# Check if log group already exists
EXISTING_LOG_GROUP=$(aws logs describe-log-groups \
    --region "${AWS_REGION}" \
    --log-group-name-prefix "${LOG_GROUP_NAME}" \
    --query "logGroups[?logGroupName=='${LOG_GROUP_NAME}'].logGroupName" \
    --output text 2>/dev/null || echo "")

if [ "${EXISTING_LOG_GROUP}" == "${LOG_GROUP_NAME}" ]; then
    echo "Log group already exists: ${LOG_GROUP_NAME}"
else
    echo "Creating log group: ${LOG_GROUP_NAME}"
    aws logs create-log-group \
        --log-group-name "${LOG_GROUP_NAME}" \
        --region "${AWS_REGION}"
    echo "Created: ${LOG_GROUP_NAME}"
fi

# Set retention policy (idempotent - safe to run multiple times)
echo "Setting log retention to ${RETENTION_DAYS} days..."
aws logs put-retention-policy \
    --log-group-name "${LOG_GROUP_NAME}" \
    --retention-in-days "${RETENTION_DAYS}" \
    --region "${AWS_REGION}"

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "  LOG_GROUP_NAME=${LOG_GROUP_NAME}"
echo "  RETENTION=${RETENTION_DAYS} days"
