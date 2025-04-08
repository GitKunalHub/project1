import time
import socket
import pika
import os
import json
import pandas as pd
import tiktoken
from datetime import datetime
from processor.app.tasks import process_topic_batch, MAIN_QUEUE, DLQ_QUEUE, MAIN_EXCHANGE, DLX_EXCHANGE
from processor.app.topic_extraction import Config, read_azure_file, save_analysis_results, consolidate_topics_in_json
import openai

LOG_FILE = "producer_ran.log"

import redis
import json

def get_interactions_from_redis():
    """Retrieve all interactions from the Redis queue."""
    redis_client = redis.Redis(host='redis', port=6379, db=0)
    interactions = []
    while True:
        # Using RPOP to get interactions from the end of the list
        interaction_data = redis_client.rpop("interactions_queue")
        if not interaction_data:
            break
        interactions.append(json.loads(interaction_data))
    return interactions

def log_message(message):
    """Append logs to the file for tracking."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} - {message}\n")

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

def declare_queues():
    """Declare RabbitMQ queues and exchanges."""
    try:
        credentials = pika.PlainCredentials('myuser', 'mypassword')
        parameters = pika.ConnectionParameters(
            host='rabbitmq',
            port=5672,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)        
        channel = connection.channel()

        # Create exchanges
        channel.exchange_declare(
            exchange=MAIN_EXCHANGE.name,
            exchange_type=MAIN_EXCHANGE.type,
            durable=True,
        )
        channel.exchange_declare(
            exchange=DLX_EXCHANGE.name,
            exchange_type=DLX_EXCHANGE.type,
            durable=True,
        )

        # Delete existing queues (if any)
        channel.queue_delete(MAIN_QUEUE)
        channel.queue_delete(DLQ_QUEUE)

        # Create main queue with DLX
        channel.queue_declare(
            queue=MAIN_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE.name,
                "x-dead-letter-routing-key": DLQ_QUEUE,
            },
        )

        # Create DLQ
        channel.queue_declare(
            queue=DLQ_QUEUE,
            durable=True,
        )
        # Declare interactions_ready as durable and do NOT purge it
        channel.queue_declare(
            queue='interactions_ready',
            durable=True,  # Survive broker restarts
        )

        # Bind queues to exchanges
        channel.queue_bind(
            exchange=MAIN_EXCHANGE.name,
            queue=MAIN_QUEUE,
            routing_key=MAIN_QUEUE,
        )
        channel.queue_bind(
            exchange=DLX_EXCHANGE.name,
            queue=DLQ_QUEUE,
            routing_key=DLQ_QUEUE,
        )

        connection.close()
        print("Queues and exchanges configured successfully.")
    except Exception as e:
        print(f"Queue setup failed: {e}")
        raise

def send_batch(batch_ids):
    """Send a batch of interaction IDs to the task queue."""
    return process_topic_batch.delay(batch_ids)

def wait_for_main_queue_empty():
    """Wait until the main queue has 0 messages."""
    while True:
        credentials = pika.PlainCredentials('myuser', 'mypassword')
        parameters = pika.ConnectionParameters(
            host='rabbitmq',
            port=5672,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)        
        channel = connection.channel()
        main_queue = channel.queue_declare(MAIN_QUEUE, passive=True)
        if main_queue.method.message_count == 0:
            connection.close()
            return
        print(f"Main queue has {main_queue.method.message_count} pending messages...")
        connection.close()
        time.sleep(5)

def wait_for_interactions_ready():
    """Wait for the interactions_ready message with a timeout."""
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    parameters = pika.ConnectionParameters(
        host='rabbitmq',
        port=5672,
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)        
    channel = connection.channel()
    
    print("⏳ Waiting for 'interactions_ready' message (timeout: 2 minutes)...")
    
    try:
        # Use basic_get instead of basic_consume to avoid blocking indefinitely
        method_frame, properties, body = channel.basic_get(queue='interactions_ready', auto_ack=False)
        
        if method_frame:
            print("✅ Received 'interactions_ready' message.")
            channel.basic_ack(method_frame.delivery_tag)
        else:
            # If no message, wait with a timeout
            def callback(ch, method, properties, body):
                ch.basic_ack(method.delivery_tag)
                print("✅ Received 'interactions_ready' message via callback.")
                ch.stop_consuming()
            
            channel.basic_consume(queue='interactions_ready', on_message_callback=callback)
            connection.call_later(60, lambda: channel.stop_consuming())  # 2-minute timeout
            channel.start_consuming()
    except Exception as e:
        print("Oh no, error:", e)

            
    # finally:
        # connection.close()
### Token-based Batching Functions ###

def count_tokens(text, encoding="", default_encoder="cl100k_base"):
    if not encoding:
        encoding = tiktoken.get_encoding(default_encoder)
    return len(encoding.encode(text))

def find_max_records_within_limit_custom(data, encoding, max_tokens, reserved_model_response_tokens):
    """
    Iterates over records and selects a sequence of record IDs
    that keep the running token count within the max_tokens limit.
    Uses the record's 'data' field for token counting.
    """
    static_tokens = reserved_model_response_tokens  # reserved tokens for output
    running_token_count = static_tokens
    valid_ids = []
    for record in data:
        record_tokens = count_tokens(record["interactions"], encoding)
        # Skip record if it alone exceeds the limit
        if record_tokens > max_tokens - static_tokens:
            print(f"Skipping record {record['id']}: Exceeds token limit")
            continue
        if running_token_count + record_tokens > max_tokens:
            break
        running_token_count += record_tokens
        valid_ids.append(record["interaction_id"])
    return valid_ids

def process_records_in_batches_by_session(records, max_tokens=4096, reserved_model_response_tokens=200, encoding_name="cl100k_base"):
    """
    Process records into batches while keeping interactions of the same session_id together.
    """
    encoding = tiktoken.get_encoding(encoding_name)
    batches = []
    current_batch = []
    running_token_count = reserved_model_response_tokens

    # Group records by session_id
    sessions = {}
    for record in records:
        session = record["session_id"]
        sessions.setdefault(session, []).append(record)

    # Optional: sort sessions if needed (e.g., by earliest timestamp or session_id)
    sorted_sessions = sorted(sessions.items(), key=lambda x: x[0])

    for session_id, group in sorted_sessions:
        # Calculate total tokens for this session group
        session_token_count = sum(count_tokens(rec["interactions"], encoding) for rec in group)
        
        # If the session by itself exceeds the token limit, handle it record by record
        if session_token_count > max_tokens - reserved_model_response_tokens:
            for rec in group:
                rec_tokens = count_tokens(rec["interactions"], encoding)
                # Flush current batch if adding the record exceeds limit
                if running_token_count + rec_tokens > max_tokens:
                    if current_batch:
                        batches.append([r["interaction_id"] for r in current_batch])
                    current_batch = [rec]
                    running_token_count = reserved_model_response_tokens + rec_tokens
                else:
                    current_batch.append(rec)
                    running_token_count += rec_tokens
        else:
            # If adding the whole session doesn't exceed the limit, add the group
            if running_token_count + session_token_count > max_tokens:
                if current_batch:
                    batches.append([r["interaction_id"] for r in current_batch])
                current_batch = group.copy()
                running_token_count = reserved_model_response_tokens + session_token_count
            else:
                current_batch.extend(group)
                running_token_count += session_token_count

    if current_batch:
        batches.append([r["interaction_id"] for r in current_batch])
        
    return batches


def process_records_in_batches_custom(records, max_tokens=4096, reserved_model_response_tokens=200, encoding_name="cl100k_base"):
    """
    Processes a list of records into batches where each batch's token count (based on 'data')
    does not exceed the max_tokens limit.
    Returns a list of batches, each being a list of record IDs.
    """
    encoding = tiktoken.get_encoding(encoding_name)
    batches = []
    start_index = 0

    while start_index < len(records):
        batch_ids = find_max_records_within_limit_custom(
            records[start_index:],
            encoding,
            max_tokens,
            reserved_model_response_tokens,
        )

        if not batch_ids:
            print(f"Skipping record at index {start_index}: No valid records found")
            start_index += 1
            continue

        batches.append(batch_ids)
        start_index += len(batch_ids)
    return batches

if __name__ == "__main__":
    open("producer_ran.flag", "w").close()
    # IMPORTANT: Ensure no Celery worker is running while declaring queues.
    wait_for_rabbitmq("rabbitmq", 5672)
    declare_queues()
    # Read input data from Azure.
    try:
        data = get_interactions_from_redis()
        if not data:
            print("No interactions found in Redis!")
            # Optionally, you could add a fallback to read from Azure if Redis is empty.
        else:
            print(f"Retrieved {len(data)} interactions from Redis.")
    except Exception as e:
        log_message(f"❌ Failed to read input file from Azure: {e}")
        print(f"Failed to read input file from Azure: {e}")
        raise

    # Use the token-based batching logic.
    # Here, we're processing the full list of records (each record is a dict with "id" and "data")
    # and returning batches (lists of IDs) that satisfy the token count limit.
    batches = process_records_in_batches_by_session(
        records=data,
        max_tokens=4096,                  # set your desired max tokens
        reserved_model_response_tokens=200,  # reserved tokens for model output
        encoding_name="cl100k_base"
    )
    
    all_tasks = []
    for i, batch_ids in enumerate(batches):
        task = send_batch(batch_ids)
        all_tasks.append(task)
        log_message(f"📤 Sent batch {i+1} with {len(batch_ids)} record IDs.")
        time.sleep(0.5)

    # Collect results.
    all_results = []
    for task in all_tasks:
        try:
            res = task.get(timeout=260)
            log_message(f"✅ Task result: {json.dumps(res, indent=2)}")
            print(f"Task result: {res}")
            all_results.extend(res)
        except Exception as e:
            log_message(f"❌ Task failed: {e}")
            print(f"Task failed: {e}")
    
    print("Waiting for main queue to drain...")
    wait_for_main_queue_empty()

    # Now, DLQ tasks will be automatically picked up by the worker!
    print("DLQ tasks will be processed automatically by the worker.")

