import subprocess
import sys
import time
import signal
import redis
import pika  # For waiting on the interactions message
from processor.app.producer import (
    wait_for_rabbitmq,
    declare_queues,
    wait_for_main_queue_empty
)

def wait_for_redis():
    """Wait for Redis to become available."""
    print("🌟 Waiting for Redis to be ready...")
    r = redis.Redis(host='redis', port=6379)
    start = time.time()
    timeout = 30
    while True:
        try:
            if r.ping():
                print("Redis is up!")
                return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Timeout waiting for Redis") from e
            print("Waiting for Redis...")
            time.sleep(2)

def wait_for_interactions_ready(timeout=160):
    """
    Wait for the 'Interactions ready' message from RabbitMQ.
    This function polls the 'interactions_ready' queue until it finds the message.
    """
    print("🌟 Waiting for 'Interactions ready' message from RabbitMQ...")
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # # Declare the queue to ensure it exists
    # channel.queue_declare(queue='interactions_ready', durable=True)
    start = time.time()
    
    while True:
        method_frame, header_frame, body = channel.basic_get(queue='message_queue', auto_ack=True)
        if method_frame:
            message = body.decode('utf-8')
            # Assuming the expected message is "Interactions ready" or simply "ready"
            if message.lower() in ["interactions ready", "ready"]:
                print("✅ Received 'Interactions ready' message!")
                connection.close()
                return
            else:
                print(f"⚠️ Unexpected message received: {message}")
        if time.time() - start > timeout:
            connection.close()
            raise Exception("Timeout waiting for 'Interactions ready' message.")
        time.sleep(1)

def main():
    try:
        processes = []
 
        print("🌟 Waiting for RabbitMQ to be ready...")
        wait_for_rabbitmq('rabbitmq', 5672)
        wait_for_redis()

        # Wait for the interactions-ready signal before proceeding
        wait_for_interactions_ready()

        # Setup
        print("🔧 Declaring queues and exchanges...")
        declare_queues()

        print("🚀 Starting main queue worker...")
        main_worker = subprocess.Popen([
            "celery", "-A", "processor.app.tasks", "worker",
            "--loglevel=info", "-Q", "main_queue", "--concurrency=4"
        ])
        processes.append(main_worker)

        print("📤 Running producer...")
        subprocess.run([sys.executable, "-m", "processor.app.producer"], check=True)

        print("⏳ Waiting for main queue to drain...")
        wait_for_main_queue_empty()

        print("✨ Starting DLQ worker...")
        dlq_worker = subprocess.Popen([
            "celery", "-A", "processor.app.tasks", "worker",
            "--loglevel=info", "-Q", "custom_dlq", "--concurrency=1"
        ])
        processes.append(dlq_worker)

        print("🎉 Waiting for workers to finish...")
        main_worker.wait()
        dlq_worker.wait()
    except Exception as e:
        print(f"Oops, something went wrong: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
