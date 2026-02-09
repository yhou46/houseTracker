#!/bin/bash
set -e

echo "========================================"
echo "Data Ingestion Service Starting"
echo "========================================"
echo "Working Directory: $(pwd)"
echo "Python Version: $(python --version)"
echo "========================================"

exec python data_service/property_data_ingestion_service.py
