#!/bin/bash
set -e

# Configuration (must match create_registries.sh)
AWS_REGION="us-west-2"
REPOSITORY_PREFIX="housetracker"

# Repository names
REDFIN_SPIDER_REPO="${REPOSITORY_PREFIX}/redfin-spider"
DATA_INGESTION_REPO="${REPOSITORY_PREFIX}/data-ingestion-service"
PROPERTY_SCAN_REPO="${REPOSITORY_PREFIX}/property-scan-service"

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Function to generate date-based tag with auto-incrementing number
# Format: YYYYmmdd.<number> (e.g., 20260208.0, 20260208.1)
generate_date_tag() {
    local repo_name=$1
    local date_prefix
    date_prefix=$(date -u +"%Y%m%d")

    # Get existing tags for this repo that match today's date
    existing_tags=$(aws ecr list-images \
        --repository-name "${repo_name}" \
        --region "${AWS_REGION}" \
        --query "imageIds[*].imageTag" \
        --output text 2>/dev/null \
        | tr '\t' '\n' \
        | grep "^${date_prefix}\." || echo "")

    # Find the highest number for today's date
    max_number=-1
    for tag in ${existing_tags}; do
        # Extract number after the date prefix (e.g., "20260208.5" -> "5")
        number="${tag##*.}"
        if [[ "${number}" =~ ^[0-9]+$ ]] && [ "${number}" -gt "${max_number}" ]; then
            max_number="${number}"
        fi
    done

    # Increment to get next number
    next_number=$((max_number + 1))
    echo "${date_prefix}.${next_number}"
}

# Determine image tag
if [ -n "$1" ]; then
    # Use provided tag
    IMAGE_TAG="$1"
    echo "Using provided tag: ${IMAGE_TAG}"
else
    # Generate date-based tag (check against first repo)
    IMAGE_TAG=$(generate_date_tag "${REDFIN_SPIDER_REPO}")
    echo "Generated date-based tag: ${IMAGE_TAG}"
fi

echo "=========================================="
echo "Step 1: Authenticate Docker with ECR"
echo "=========================================="
aws ecr get-login-password --region "${AWS_REGION}" | \
    docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo ""
echo "=========================================="
echo "Step 2: Build Docker images"
echo "=========================================="
DOCKERFILE_DIR="$(dirname "$0")/../../python"

echo "Building redfin-spider image..."
docker build \
    --platform linux/arm64 \
    --target redfin-spider \
    --tag "${REDFIN_SPIDER_REPO}:${IMAGE_TAG}" \
    --file "${DOCKERFILE_DIR}/Dockerfile" \
    "${DOCKERFILE_DIR}"

echo ""
echo "Building data-ingestion-service image..."
docker build \
    --platform linux/arm64 \
    --target data-ingestion-service \
    --tag "${DATA_INGESTION_REPO}:${IMAGE_TAG}" \
    --file "${DOCKERFILE_DIR}/Dockerfile" \
    "${DOCKERFILE_DIR}"

echo ""
echo "Building property-scan-service image..."
docker build \
    --platform linux/arm64 \
    --target property-scan-service \
    --tag "${PROPERTY_SCAN_REPO}:${IMAGE_TAG}" \
    --file "${DOCKERFILE_DIR}/Dockerfile" \
    "${DOCKERFILE_DIR}"

echo ""
echo "=========================================="
echo "Step 3: Tag images for ECR"
echo "=========================================="
echo "Tagging redfin-spider..."
docker tag "${REDFIN_SPIDER_REPO}:${IMAGE_TAG}" \
    "${ECR_REGISTRY}/${REDFIN_SPIDER_REPO}:${IMAGE_TAG}"

echo "Tagging data-ingestion-service..."
docker tag "${DATA_INGESTION_REPO}:${IMAGE_TAG}" \
    "${ECR_REGISTRY}/${DATA_INGESTION_REPO}:${IMAGE_TAG}"

echo "Tagging property-scan-service..."
docker tag "${PROPERTY_SCAN_REPO}:${IMAGE_TAG}" \
    "${ECR_REGISTRY}/${PROPERTY_SCAN_REPO}:${IMAGE_TAG}"

echo ""
echo "=========================================="
echo "Step 4: Push images to ECR"
echo "=========================================="
echo "Pushing redfin-spider..."
docker push "${ECR_REGISTRY}/${REDFIN_SPIDER_REPO}:${IMAGE_TAG}"

echo ""
echo "Pushing data-ingestion-service..."
docker push "${ECR_REGISTRY}/${DATA_INGESTION_REPO}:${IMAGE_TAG}"

echo ""
echo "Pushing property-scan-service..."
docker push "${ECR_REGISTRY}/${PROPERTY_SCAN_REPO}:${IMAGE_TAG}"

echo ""
echo "=========================================="
echo "Done!"
echo "=========================================="
echo ""
echo "Pushed images:"
echo "  ${ECR_REGISTRY}/${REDFIN_SPIDER_REPO}:${IMAGE_TAG}"
echo "  ${ECR_REGISTRY}/${DATA_INGESTION_REPO}:${IMAGE_TAG}"
echo "  ${ECR_REGISTRY}/${PROPERTY_SCAN_REPO}:${IMAGE_TAG}"
