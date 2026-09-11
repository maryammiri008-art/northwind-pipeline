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
#!/usr/bin/env bash
set +e
cd "${PROJECT_HOME:-$HOME/de_final_project}"
for f in logs/producer.pid logs/consumer.pid; do
if [ -f "$f" ]; then
kill "$(cat "$f")" 2>/dev/null
rm -f "$f"
fi
done
echo "Streaming processes stopped."
chmod +x scripts/start_streaming.sh scripts/stop_streaming.sh scripts/run_airflow.sh
./scripts/start_streaming.sh
