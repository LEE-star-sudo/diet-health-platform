#!/bin/bash

# 等待 Docker 和 Flink 容器完全就绪（最长等 60 秒）
echo "Checking Flink JobManager readiness..."
for i in {1..30}; do
    if curl -s http://localhost:8081/overview | grep -q '"slots-total"'; then
        echo "Flink cluster is ready!"
        break
    fi
    sleep 2
done

# 检查当前作业是否已经在运行，防止重复提交
RUNNING_JOB=$(docker exec flink-jobmanager flink list 2>/dev/null | grep -i "MealDailySummaryRealtimeAggregationJob" | grep -i "RUNNING")

if [ -n "$RUNNING_JOB" ]; then
    echo "MealDailySummaryJob is already running. Skip submission."
    exit 0
fi

if [ -z "${MYSQL_PASSWORD:-}" ]; then
    echo "MYSQL_PASSWORD is required"
    exit 1
fi

echo "Copying jar and submitting job..."
docker exec flink-jobmanager mkdir -p /opt/flink/checkpoints/meal-daily-summary
docker exec flink-taskmanager mkdir -p /opt/flink/checkpoints/meal-daily-summary
docker exec flink-jobmanager chown -R flink:flink /opt/flink/checkpoints
docker exec flink-taskmanager chown -R flink:flink /opt/flink/checkpoints
docker cp /home/ubuntu/meal-flink-job/target/meal-flink-job-1.0.0.jar flink-jobmanager:/opt/flink/
docker exec -e MYSQL_PASSWORD="$MYSQL_PASSWORD" flink-jobmanager flink run -d /opt/flink/meal-flink-job-1.0.0.jar

echo "Flink Job submitted successfully."
