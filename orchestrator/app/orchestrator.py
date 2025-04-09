import pika
import time
import redis
import socket

def check_processor_availability():
    r = redis.Redis(host='redis', port=6379)
    return r.get('processor_available') == b'1'

def wait_for_rabbitmq(host, port, timeout=30):
    """Wait until RabbitMQ is available."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("RabbitMQ is up!")
                return
        except Exception as e:
            if time.time() - start_time > timeout:
                print("Timeout waiting for RabbitMQ!")
                raise
            print("Waiting for RabbitMQ...")
            time.sleep(2)

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

def orchestrator():
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare the FIFO queue
    channel.queue_declare(queue='processor_queue', durable=True)

    while True:
        print("Hello from orchestrator!")
        method_frame, header_frame, body = channel.basic_get(queue='message_queue', auto_ack=False)
        if method_frame:
            message = body.decode('utf-8')
            if check_processor_availability():
                # Send message to the processor queue
                channel.basic_publish(exchange='', routing_key='processor_queue', body=message)
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                print(f"Message '{message}' sent to processor.")
            else:
                print("Processor not available, message requeued.")
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
        time.sleep(5)

if __name__ == "__main__":
    print("🌟 Waiting for RabbitMQ to be ready...")
    wait_for_rabbitmq('rabbitmq', 5672)
    wait_for_redis()
    orchestrator()