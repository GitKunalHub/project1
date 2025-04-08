#!/bin/bash
set -e

echo "🌟 Waiting for RabbitMQ to be ready..."
python -c "from app.producer import wait_for_rabbitmq; wait_for_rabbitmq('rabbitmq', 5672)"

echo "🌟 Waiting for Redis to be ready..."
python << 'EOF'
import redis, time
r = redis.Redis(host='redis', port=6379)
start = time.time()
timeout = 30
while True:
    try:
        if r.ping():
            print("Redis is up!")
            break
    except Exception:
        pass
    if time.time() - start > timeout:
        raise Exception("Timeout waiting for Redis")
    print("Waiting for Redis...")
    time.sleep(2)
EOF

echo "🔧 Declaring queues and exchanges..."
python -c "from app.producer import declare_queues; declare_queues()"

echo "🚀 Starting main queue worker (4 concurrency)..."
celery -A app.tasks worker --loglevel=info -Q main_queue --concurrency=4 &
MAIN_WORKER_PID=$!

echo "📤 Running producer to send tasks..."
python -m processor.app.producer

echo "⏳ Waiting for main queue to drain..."
python -c "from app.producer import wait_for_main_queue_empty; wait_for_main_queue_empty()"

echo "✨ Starting DLQ worker (single concurrency) for failed tasks..."
celery -A app.tasks worker --loglevel=info -Q custom_dlq --concurrency=1 &
DLQ_WORKER_PID=$!

echo "🎉 All workers are running. Waiting for them to finish..."
wait $MAIN_WORKER_PID $DLQ_WORKER_PID
