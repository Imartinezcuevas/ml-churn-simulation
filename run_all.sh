#!/bin/bash

echo "🚀 Starting all Kafka consumers and orchestrator..."

trap "echo '🧹 Stopping all processes...'; pkill -f 'python consumers'; pkill -f 'python producers/orchestrator.py'; exit 0" SIGINT SIGTERM

# Start consumers in background
python consumers/signups_consumer.py &
python consumers/activity_consumer.py &
python consumers/billing_consumer.py &
python consumers/support_consumer.py &

# Small delay to let consumers connect
sleep 5

# Start orchestrator
python producers/orchestrator.py

# Wait for background jobs
wait
