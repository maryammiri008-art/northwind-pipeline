#!/bin/bash
export AIRFLOW_HOME=~/de_final_project/airflow
cd ~/de_final_project
source venv/bin/activate

pkill -f "airflow" 2>/dev/null

mkdir -p logs airflow/dags

airflow db migrate

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin 2>/dev/null || echo "✅ User already exists"

nohup airflow scheduler > logs/scheduler.log 2>&1 &
echo $! > logs/scheduler.pid

nohup airflow webserver --port 8080 > logs/webserver.log 2>&1 &
echo $! > logs/webserver.pid

echo "✅ Airflow started!"
echo "📋 Scheduler PID: $(cat logs/scheduler.pid)"
echo "📋 Webserver PID: $(cat logs/webserver.pid)"
echo "🌐 Open: http://localhost:8080"
