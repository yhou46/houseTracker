#!/bin/bash
set -e

# Usage:
#   ./deploy.sh                        # push images + register task definition
#   ./deploy.sh --skip-task-def        # push images only
#   ./deploy.sh --tag 20260322.0       # use a specific tag
#   ./deploy.sh --tag 20260322.0 --skip-task-def

SCRIPT_DIR="$(dirname "$0")"

IMAGE_TAG=""
SKIP_TASK_DEF=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --skip-task-def)
            SKIP_TASK_DEF=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--tag <tag>] [--skip-task-def]"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Deploy"
echo "=========================================="
echo ""
echo "  skip-task-def: ${SKIP_TASK_DEF}"
echo ""

echo "=========================================="
echo "Step 1: Push Images"
echo "=========================================="
source "${SCRIPT_DIR}/push_images.sh" ${IMAGE_TAG:+"$IMAGE_TAG"}

echo ""
echo "Image tag: ${IMAGE_TAG}"

if [ "${SKIP_TASK_DEF}" = true ]; then
    echo ""
    echo "Skipping task definition registration (--skip-task-def)"
else
    echo ""
    echo "=========================================="
    echo "Step 2: Register Task Definition"
    echo "=========================================="
    bash "${SCRIPT_DIR}/setup_ecs_task_definition.sh" "${IMAGE_TAG}"
fi

echo ""
echo "=========================================="
echo "Done"
echo "=========================================="
