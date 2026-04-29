#!/bin/bash
set -e

# Configuration
AWS_REGION="us-west-2"
REPOSITORY_PREFIX="housetracker"

# Repository names
REDFIN_SPIDER_REPO="${REPOSITORY_PREFIX}/redfin-spider"
DATA_INGESTION_REPO="${REPOSITORY_PREFIX}/data-ingestion-service"
PROPERTY_SCAN_REPO="${REPOSITORY_PREFIX}/property-scan-service"

# Function to create repository if it doesn't exist
create_repo_if_not_exists() {
    local repo_name=$1

    if aws ecr describe-repositories --repository-names "${repo_name}" --region "${AWS_REGION}" &> /dev/null; then
        echo "Repository already exists: ${repo_name}"
    else
        echo "Creating repository: ${repo_name}"
        aws ecr create-repository \
            --repository-name "${repo_name}" \
            --region "${AWS_REGION}" \
            --image-scanning-configuration scanOnPush=true \
            --image-tag-mutability IMMUTABLE \
            --output table
    fi
}

echo "Creating ECR repositories in region: ${AWS_REGION}"
echo ""

create_repo_if_not_exists "${REDFIN_SPIDER_REPO}"
create_repo_if_not_exists "${DATA_INGESTION_REPO}"
create_repo_if_not_exists "${PROPERTY_SCAN_REPO}"

echo ""
echo "ECR repositories created successfully!"
echo ""
echo "Repository URIs:"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REDFIN_SPIDER_REPO}"
echo "  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${DATA_INGESTION_REPO}"
echo "  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROPERTY_SCAN_REPO}"
