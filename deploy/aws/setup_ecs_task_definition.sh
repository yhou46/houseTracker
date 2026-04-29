#!/bin/bash
set -e

# Usage: setup_ecs_task_definition.sh <image-tag>
# Example: setup_ecs_task_definition.sh 20260322.0
IMAGE_TAG="${1:?Usage: $0 <image-tag>}"

# Configuration
AWS_REGION="us-west-2"
TASK_FAMILY="housetracker-daily-scan"
LOG_GROUP_NAME="/ecs/housetracker"
SCRIPT_DIR="$(dirname "$0")"
TASK_DEFINITION_FILE="${SCRIPT_DIR}/config/ecs_task_definition.json"

# Derived values
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Export variables for envsubst to substitute into the JSON template
export AWS_REGION
export LOG_GROUP_NAME
export TASK_EXECUTION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/housetracker-ecs-task-execution-role"
export TASK_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/housetracker-ecs-task-role"
export REDFIN_SPIDER_IMAGE="${ECR_REGISTRY}/housetracker/redfin-spider:${IMAGE_TAG}"
export DATA_INGESTION_IMAGE="${ECR_REGISTRY}/housetracker/data-ingestion-service:${IMAGE_TAG}"
export PROPERTY_SCAN_IMAGE="${ECR_REGISTRY}/housetracker/property-scan-service:${IMAGE_TAG}"

echo "=========================================="
echo "Registering ECS Task Definition"
echo "=========================================="
echo ""
echo "Task family: ${TASK_FAMILY}"
echo "Template:    ${TASK_DEFINITION_FILE}"
echo "Images:"
echo "  redfin-spider:          ${REDFIN_SPIDER_IMAGE}"
echo "  data-ingestion-service: ${DATA_INGESTION_IMAGE}"
echo "  property-scan-service:  ${PROPERTY_SCAN_IMAGE}"
echo ""

# Substitute environment variables into the JSON template
TASK_DEFINITION=$(envsubst < "${TASK_DEFINITION_FILE}")

# Register task definition (each run creates a new revision)
TASK_DEF_ARN=$(aws ecs register-task-definition \
    --region "${AWS_REGION}" \
    --cli-input-json "${TASK_DEFINITION}" \
    --tags "key=image-tag,value=${IMAGE_TAG}" \
    --query "taskDefinition.taskDefinitionArn" \
    --output text)

echo "Task definition registered: ${TASK_DEF_ARN}"
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "  TASK_FAMILY=${TASK_FAMILY}"
echo "  TASK_DEF_ARN=${TASK_DEF_ARN}"
