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

echo "Building pipeline and streamlit images..."
docker compose build --quiet

echo ""
echo "Starting all services..."
docker compose up -d

echo ""
echo "========================================================"
echo "  Services are starting. URLs:"
echo "  ⚡ Streamlit Dashboard  → http://localhost:8501"
echo "  🐘 Hadoop NameNode UI   → http://localhost:9870"
echo "  🧶 YARN ResourceManager → http://localhost:8088"
echo "  📦 Mongo Express        → http://localhost:8081"
echo "     (login: admin / admin123)"
echo "========================================================"
echo ""
echo "Pipeline logs (MapReduce progress):"
echo "  docker logs -f pipeline"
echo ""
echo "To stop everything:"
echo "  docker compose down"
echo ""

# Optionally tail pipeline logs
read -p "Follow pipeline logs now? [y/N] " choice
if [[ "$choice" =~ ^[Yy]$ ]]; then
    echo "Waiting for pipeline container to start..."
    sleep 5
    docker logs -f pipeline
fi
