#!/bin/bash
# This script sets up the development environment for this project.

# Set up python path
python_root="$(cd "$(dirname "$0")" && pwd)"
echo "Python project root: $python_root"

echo "Set up python path..."
export PYTHONPATH="$python_root:$PYTHONPATH"
echo "PYTHONPATH is set to: $PYTHONPATH"

echo "Set up AWS credentials..."
echo "list available AWS profiles:"
profile_name=$(aws configure list-profiles)
echo "$profile_name"
export AWS_PROFILE="$profile_name"

echo "AWS_PROFILE is set to: $AWS_PROFILE"
aws sso login
echo "Logged in to AWS SSO."
echo "Development environment setup complete."