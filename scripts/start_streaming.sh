#!/bin/bash
export PROJECT_HOME=~/de_final_project
cd $PROJECT_HOME
mkdir -p logs

nohup ./venv/bin/python kafka/producer.py > logs/producer.log 2>&1 &
echo $! > logs/producer.pid

nohup ./venv/bin/python kafka/consumer.py > logs/consumer.log 2>&1 &
echo $! > logs/consumer.pid

echo "✅ Producer PID: $(cat logs/producer.pid)"
echo "✅ Consumer PID: $(cat logs/consumer.pid)"
echo "📋 Use: tail -f logs/producer.log"
echo "📋 Use: tail -f logs/consumer.log"
