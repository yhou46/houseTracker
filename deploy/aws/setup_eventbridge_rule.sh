#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
CLUSTER_NAME="housetracker"
TASK_FAMILY="housetracker-daily-scan"
RULE_NAME="housetracker-daily-scan"
SECURITY_GROUP_NAME="housetracker-ecs-tasks-sg"
EVENTBRIDGE_ROLE_NAME="housetracker-eventbridge-ecs-role"

# 18:00 PT:
#   PST (Nov-Mar): UTC-8  → 02:00 UTC  cron(0 2 * * ? *)
#   PDT (Mar-Nov): UTC-7  → 01:00 UTC  cron(0 1 * * ? *)
# Using PST (02:00 UTC). During summer the task runs at 19:00 PT instead of 18:00.
# SCHEDULE_EXPRESSION="cron(0 18 * * ? *)"

# Temporary: test run at 19:55 PT
SCHEDULE_EXPRESSION="cron(00 18 * * ? *)"

echo "=========================================="
echo "Setup EventBridge Rule for Daily ECS Task"
echo "=========================================="
echo ""

# Derived values
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "=========================================="
echo "Step 1: Look Up Networking"
echo "=========================================="
DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
    --region "${AWS_REGION}" \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text)

SUBNET_IDS=$(aws ec2 describe-subnets \
    --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "Subnets[*].SubnetId" \
    --output text)

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
    --region "${AWS_REGION}" \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "SecurityGroups[0].GroupId" \
    --output text)

# Format subnet IDs as JSON array for the ECS network config
SUBNET_JSON=$(echo "${SUBNET_IDS}" | tr '\t' '\n' | jq -R . | jq -s .)

echo "VPC:            ${DEFAULT_VPC_ID}"
echo "Subnets:        ${SUBNET_IDS}"
echo "Security Group: ${SECURITY_GROUP_ID}"

echo ""
echo "=========================================="
echo "Step 2: Get Latest Task Definition ARN"
echo "=========================================="
TASK_DEF_ARN=$(aws ecs describe-task-definition \
    --region "${AWS_REGION}" \
    --task-definition "${TASK_FAMILY}" \
    --query "taskDefinition.taskDefinitionArn" \
    --output text)

echo "Found task definition: ${TASK_DEF_ARN}"

echo ""
echo "=========================================="
echo "Step 3: Look Up EventBridge IAM Role"
echo "=========================================="
EVENTBRIDGE_ROLE_ARN=$(aws iam get-role \
    --role-name "${EVENTBRIDGE_ROLE_NAME}" \
    --query "Role.Arn" \
    --output text)

echo "Found EventBridge role: ${EVENTBRIDGE_ROLE_ARN}"

echo ""
echo "=========================================="
echo "Step 4: Create EventBridge Scheduler"
echo "=========================================="

ECS_TARGET=$(cat <<EOF
{
    "Arn": "arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:cluster/${CLUSTER_NAME}",
    "RoleArn": "${EVENTBRIDGE_ROLE_ARN}",
    "EcsParameters": {
        "TaskDefinitionArn": "${TASK_DEF_ARN}",
        "TaskCount": 1,
        "LaunchType": "FARGATE",
        "NetworkConfiguration": {
            "awsvpcConfiguration": {
                "Subnets": ${SUBNET_JSON},
                "SecurityGroups": ["${SECURITY_GROUP_ID}"],
                "AssignPublicIp": "ENABLED"
            }
        }
    }
}
EOF
)

EXISTING_SCHEDULE=$(aws scheduler get-schedule \
    --name "${RULE_NAME}" \
    --region "${AWS_REGION}" \
    --query "Name" \
    --output text 2>/dev/null || echo "None")

if [ "${EXISTING_SCHEDULE}" != "None" ] && [ -n "${EXISTING_SCHEDULE}" ]; then
    echo "Updating existing schedule: ${RULE_NAME}"
    aws scheduler update-schedule \
        --name "${RULE_NAME}" \
        --region "${AWS_REGION}" \
        --schedule-expression "${SCHEDULE_EXPRESSION}" \
        --schedule-expression-timezone "America/Los_Angeles" \
        --flexible-time-window '{"Mode": "OFF"}' \
        --target "${ECS_TARGET}"
else
    echo "Creating schedule: ${RULE_NAME}"
    aws scheduler create-schedule \
        --name "${RULE_NAME}" \
        --region "${AWS_REGION}" \
        --schedule-expression "${SCHEDULE_EXPRESSION}" \
        --schedule-expression-timezone "America/Los_Angeles" \
        --flexible-time-window '{"Mode": "OFF"}' \
        --target "${ECS_TARGET}"
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "  RULE_NAME=${RULE_NAME}"
echo "  SCHEDULE=${SCHEDULE_EXPRESSION} (18:00 PST / 19:00 PDT)"
echo "  TASK_DEF_ARN=${TASK_DEF_ARN}"
echo "  EVENTBRIDGE_ROLE_ARN=${EVENTBRIDGE_ROLE_ARN}"
echo ""
echo "The task will run daily at 18:00 PST (02:00 UTC)."
echo "Note: During daylight saving time (PDT, Mar-Nov) it runs at 19:00 PT."
echo "To run at exactly 18:00 year-round, use --schedule-expression-timezone"
echo "with 'America/Los_Angeles' (already set above - AWS Scheduler handles DST automatically)."
