#!/bin/bash
set -e

echo "========================================"
echo "Property Scan Service Starting"
echo "========================================"
echo "Working Directory: $(pwd)"
echo "Python Version: $(python --version)"
echo "========================================"

exec python data_service/property_scan_service.py
