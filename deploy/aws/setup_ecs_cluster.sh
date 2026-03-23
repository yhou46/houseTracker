#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
CLUSTER_NAME="housetracker"

echo "=========================================="
echo "Create ECS Cluster"
echo "=========================================="

# Check if cluster already exists
EXISTING_CLUSTER=$(aws ecs describe-clusters \
    --clusters "${CLUSTER_NAME}" \
    --region "${AWS_REGION}" \
    --query "clusters[?status=='ACTIVE'].clusterName" \
    --output text 2>/dev/null || echo "")

if [ "${EXISTING_CLUSTER}" == "${CLUSTER_NAME}" ]; then
    echo "Cluster already exists: ${CLUSTER_NAME}"
else
    echo "Creating cluster: ${CLUSTER_NAME}"
    aws ecs create-cluster \
        --cluster-name "${CLUSTER_NAME}" \
        --region "${AWS_REGION}" \
        --output table
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "  CLUSTER_NAME=${CLUSTER_NAME}"
