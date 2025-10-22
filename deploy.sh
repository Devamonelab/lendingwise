#!/bin/bash

echo "================================================================================"
echo "                    LendingWise - Clean Deploy Script"
echo "================================================================================"
echo

echo "Step 1: Stopping and removing old containers..."
docker-compose down
echo

echo "Step 2: Removing old containers by name (if any)..."
docker rm -f lendingwise-api 2>/dev/null || true
docker rm -f lendingwise-sqs-worker 2>/dev/null || true
docker rm -f lendingwise-cross-validation-watcher 2>/dev/null || true
docker rm -f lendingwise-all-in-one 2>/dev/null || true
echo

echo "Step 3: Building and starting all services..."
docker-compose up -d --build
echo

echo "Step 4: Waiting for services to start (30 seconds)..."
sleep 30
echo

echo "Step 5: Checking container status..."
docker-compose ps
echo

echo "Step 6: Testing API health..."
curl http://localhost:8000/
echo
echo

echo "================================================================================"
echo "                           Deployment Complete!"
echo "================================================================================"
echo
echo "Services running:"
echo "  - API: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - SQS Worker: background"
echo "  - Cross-Validation: background"
echo
echo "View logs: docker-compose logs -f"
echo "Stop all: docker-compose down"
echo "================================================================================"

