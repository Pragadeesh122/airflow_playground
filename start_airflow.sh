#!/bin/bash

# Activate virtual environment
source ~/Developer/airflow_playground/airflow_venv/bin/activate

# Set Airflow home
export AIRFLOW_HOME=~/Developer/airflow_playground

# Start services in background
echo "Starting Airflow Scheduler..."
airflow scheduler > ~/Developer/airflow_playground/logs/scheduler.log 2>&1 &

echo "Starting Airflow Triggerer..."
airflow triggerer > ~/Developer/airflow_playground/logs/triggerer.log 2>&1 &

echo "Starting Airflow DAG Processor..."
airflow dag-processor > ~/Developer/airflow_playground/logs/dag-processor.log 2>&1 &

echo "Starting Airflow API Server..."
airflow api-server > ~/Developer/airflow_playground/logs/api-server.log 2>&1 &


echo ""
echo "All Airflow services started!"
echo ""
echo "Logs are in: ~/Developer/airflow_playground/logs/"
echo ""
echo "To stop all services, run: pkill -f airflow"