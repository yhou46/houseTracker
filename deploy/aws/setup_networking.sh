#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
SECURITY_GROUP_NAME="housetracker-ecs-tasks-sg"

echo "=========================================="
echo "Step 1: Get Default VPC"
echo "=========================================="
DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
    --region "${AWS_REGION}" \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text)

if [ "${DEFAULT_VPC_ID}" == "None" ] || [ -z "${DEFAULT_VPC_ID}" ]; then
    echo "ERROR: No default VPC found in ${AWS_REGION}"
    echo "You may need to create a VPC manually or use a different region"
    exit 1
fi

echo "Default VPC ID: ${DEFAULT_VPC_ID}"

echo ""
echo "=========================================="
echo "Step 2: Get Public Subnets"
echo "=========================================="
SUBNET_IDS=$(aws ec2 describe-subnets \
    --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "Subnets[*].SubnetId" \
    --output text)

echo "Available Subnets:"
for subnet in ${SUBNET_IDS}; do
    AZ=$(aws ec2 describe-subnets \
        --region "${AWS_REGION}" \
        --subnet-ids "${subnet}" \
        --query "Subnets[0].AvailabilityZone" \
        --output text)
    echo "  ${subnet} (${AZ})"
done

echo ""
echo "=========================================="
echo "Step 3: Create Security Group (if not exists)"
echo "=========================================="

# Check if security group already exists
EXISTING_SG=$(aws ec2 describe-security-groups \
    --region "${AWS_REGION}" \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" "Name=vpc-id,Values=${DEFAULT_VPC_ID}" \
    --query "SecurityGroups[0].GroupId" \
    --output text 2>/dev/null || echo "None")

if [ "${EXISTING_SG}" != "None" ] && [ -n "${EXISTING_SG}" ]; then
    echo "Security group already exists: ${EXISTING_SG}"
    SECURITY_GROUP_ID="${EXISTING_SG}"
else
    echo "Creating security group: ${SECURITY_GROUP_NAME}"
    SECURITY_GROUP_ID=$(aws ec2 create-security-group \
        --region "${AWS_REGION}" \
        --group-name "${SECURITY_GROUP_NAME}" \
        --description "Security group for HouseTracker ECS tasks" \
        --vpc-id "${DEFAULT_VPC_ID}" \
        --query "GroupId" \
        --output text)

    echo "Created security group: ${SECURITY_GROUP_ID}"

    # Add outbound rule for all traffic (default allows all outbound, but let's be explicit)
    # Inbound: No rules needed - tasks only make outbound requests
    echo "Security group configured (outbound: all traffic allowed by default)"
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "Save these values for ECS task configuration:"
echo ""
echo "  VPC_ID=${DEFAULT_VPC_ID}"
echo "  SUBNET_IDS=${SUBNET_IDS}"
echo "  SECURITY_GROUP_ID=${SECURITY_GROUP_ID}"
echo ""
echo "You can also export them:"
echo ""
echo "  export VPC_ID=${DEFAULT_VPC_ID}"
echo "  export SUBNET_IDS=\"${SUBNET_IDS}\""
echo "  export SECURITY_GROUP_ID=${SECURITY_GROUP_ID}"
