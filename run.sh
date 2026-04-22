#!/bin/bash
set -e

echo "========================================================"
echo "  Real-time Energy Consumption Analysis"
echo "  BDA Mini Project"
echo "========================================================"

# Verify dataset exists
if [ ! -f "household_power_consumption.csv" ]; then
    echo "ERROR: household_power_consumption.csv not found in $(pwd)"
    exit 1
fi

echo "Dataset found: $(wc -l < household_power_consumption.csv) rows"
echo ""

# Pull images first (parallel)
echo "Pulling Docker images..."
docker compose pull --quiet

echo "Building pipeline image..."
docker compose build --quiet

echo ""
echo "Starting all services..."
docker compose up -d

echo ""
echo "========================================================"
echo "  Services are starting. URLs:"
echo "  🐘 Hadoop NameNode UI   → http://localhost:9870"
echo "  🧶 YARN ResourceManager → http://localhost:8088"
echo "  📦 Mongo Express        → http://localhost:8081"
echo "     (login: admin / admin123)"
echo "========================================================"
echo ""
echo "Installing dependencies..."
pip install -r pipeline/requirements.txt

echo ""
echo "Running pipeline..."
MONGO_URI=mongodb://localhost:27017/ NAMENODE_HOST=localhost python3 pipeline/pipeline.py

echo ""
echo "Pipeline logs will be displayed in the terminal."
echo "To stop everything:"
echo "  docker compose down"
echo "  pkill -f 'python3 pipeline/pipeline.py'"
