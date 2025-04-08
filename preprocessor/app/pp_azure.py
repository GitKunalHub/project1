import os
import json
import csv
import io
import logging
import sys
import time
import socket
from typing import Any, Dict, List, Union

# Third-party libraries
from azure.storage.fileshare import ShareServiceClient
from dotenv import load_dotenv
import pymongo
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
import redis
import pika

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("azure_interactions_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AzureInteractionsProcessor")

class Config:
    # Azure File Storage
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_FILE_SHARE_NAME = os.getenv("AZURE_FILE_SHARE_NAME")
    AZURE_FILE_DIRECTORY = os.getenv("AZURE_FILE_DIRECTORY", "")
    # MongoDB
    MONGODB_URI = os.getenv("MONGODB_URL")
    MONGODB_DB = "metrices"
    MONGODB_COLLECTION = "agents"
    # RabbitMQ
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER", "myuser")
    RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "mypassword")

scheduler = BackgroundScheduler(
    jobstores={
        'default': MongoDBJobStore(
            database='metrices',
            collection='scheduler_jobs',
            client=pymongo.MongoClient(Config.MONGODB_URI))
    },
    timezone='UTC'
)

def get_directory_client():
    service_client = ShareServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    share_client = service_client.get_share_client(Config.AZURE_FILE_SHARE_NAME)
    return share_client.get_directory_client(Config.AZURE_FILE_DIRECTORY)

def read_azure_file(file_name: str) -> str:
    directory_client = get_directory_client()
    file_client = directory_client.get_file_client(file_name)
    return file_client.download_file().readall().decode('utf-8')

def write_azure_file(file_name: str, content: str) -> None:
    directory_client = get_directory_client()
    file_client = directory_client.get_file_client(file_name)
    try:
        file_client.delete_file()
    except:
        pass
    file_client.upload_file(content.encode('utf-8'))

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

def fetch_agent_config(agent_id: str) -> dict:
    try:
        with pymongo.MongoClient(Config.MONGODB_URI) as client:
            db = client[Config.MONGODB_DB]
            collection = db[Config.MONGODB_COLLECTION]
            return collection.find_one({"agent_id": agent_id}) or {}
    except Exception as e:
        logger.error(f"MongoDB Error: {e}")
        return {}
    
def get_nested(data: Any, path: str) -> Any:
    """Retrieves a nested value from data using a dot-separated path."""
    keys = path.split('.') if path else []
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and key.isdigit():
            data = data[int(key)] if int(key) < len(data) else None
        else:
            return None
        if data is None:
            return None
    return data

def apply_mapping(data: Any, mapping_spec: Union[str, Dict]) -> Any:
    """Recursively applies a mapping specification to extract values from data."""
    if isinstance(mapping_spec, str):
        return get_nested(data, mapping_spec)
    if isinstance(mapping_spec, dict):
        return {key: apply_mapping(data, spec) for key, spec in mapping_spec.items()}
    return None

def transform_log(input_data, mapping_data):
    output_data = {"conversations": []}
    
    # If input_data is a dict with a list inside, extract the list.
    if isinstance(input_data, dict):
        found = False
        for key, value in input_data.items():
            if isinstance(value, list):
                input_data = value
                found = True
                break
        if not found:
            input_data = [input_data]
    
    for conversation in input_data:
        session_id = get_nested(conversation, mapping_data["session_id"])
        transformed_conversation = {
            "session_id": session_id,
            "user_id": get_nested(conversation, mapping_data["user_id"]),
            "messages": []
        }
        
        messages = []
        # Check if mapping contains 'messages'
        if "messages" in mapping_data:
            messages_mapping = mapping_data["messages"]
            if isinstance(messages_mapping, list):
                # For each path provided, get messages and store the source path.
                for path in messages_mapping:
                    msgs = get_nested(conversation, path)
                    if isinstance(msgs, list):
                        for m in msgs:
                            m["__default_sender"] = path  # mark source for later
                        messages.extend(msgs)
                    elif msgs:
                        msgs["__default_sender"] = path
                        messages.append(msgs)
            else:
                messages = get_nested(conversation, messages_mapping)
                if not isinstance(messages, list):
                    messages = [messages]
        # Else check for 'interaction' field mapping if no separate messages exist
        elif "interaction" in mapping_data:
            interaction_mapping = mapping_data["interaction"]
            msgs = get_nested(conversation, interaction_mapping)
            if isinstance(msgs, list):
                messages = msgs
            elif msgs:
                messages = [msgs]
        else:
            # Fallback: assume entire conversation is a message list
            messages = conversation.get("messages", [])
        
        for message in messages:
            sender_path = mapping_data.get("message", {}).get("sender")
            sender = None
            if sender_path:
                sender = get_nested(message, sender_path)
            # If sender is not provided, use the default from the source path if available.
            if not sender and "__default_sender" in message:
                default = message["__default_sender"]
                if "customer" in default.lower():
                    sender = "customer"
                elif "agent" in default.lower():
                    sender = "agent"
                else:
                    sender = default
            transformed_message = {
                "sender": sender,
                "text": get_nested(message, mapping_data["message"]["text"]),
                "timestamp": get_nested(message, mapping_data["message"].get("timestamp", "")),
                "ip_address": get_nested(message, mapping_data["message"].get("ip_address", "")),
                "bot_id": get_nested(message, mapping_data["message"].get("bot_id", "")),
                "bot_name": get_nested(message, mapping_data["message"].get("bot_name", ""))
            }
            transformed_conversation["messages"].append(transformed_message)
        
        output_data["conversations"].append(transformed_conversation)
    
    return output_data

def create_interactions(conversations):
    interactions = []
    
    for convo in conversations["conversations"]:
        session_id = convo["session_id"]
        user_id = convo["user_id"]
        messages = convo["messages"]
        interaction_number = 1
        interaction_group = []
        
        for msg in messages:
            interaction_group.append({"role": msg["sender"], "message": msg["text"]})
            
            # End the interaction when bot details are present.
            if msg.get("bot_id") or msg.get("bot_name"):
                interactions.append({
                    "interaction_id": f"{session_id}_{interaction_number}",
                    "interactions": json.dumps(interaction_group),
                    "session_id": session_id,
                    "timestamp": msg["timestamp"],
                    "user_id": user_id,
                    "ip_address": msg["ip_address"],
                    "agent_id": msg["bot_id"] if msg["bot_id"] else None,
                    "agent_name": msg["bot_name"] if msg["bot_name"] else None
                })
                interaction_group = []
                interaction_number += 1
        
        # Optionally, add any leftover messages as a final interaction.
        if interaction_group:
            interactions.append({
                "interaction_id": f"{session_id}_{interaction_number}",
                "interactions": json.dumps(interaction_group),
                "session_id": session_id,
                "timestamp": messages[-1]["timestamp"],
                "user_id": user_id,
                "ip_address": messages[-1]["ip_address"],
                "agent_id": None,
                "agent_name": None
            })
    
    return interactions



def schedule_agent_processing(agent_id: str):
    agent_config = fetch_agent_config(agent_id)
    if not agent_config:
        logger.error(f"Agent {agent_id} configuration not found")
        return

    schedule_config = agent_config.get('scheduling', {})
    job_id = f"agent_{agent_id}"
    
    # Remove existing job if present
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info(f"Removed existing job for agent {agent_id}")

    try:
        schedule_type = schedule_config.get('type', 'daily')
        start_time = schedule_config.get('start_time', '00:00')

        if schedule_type == 'minutely':
            scheduler.add_job(
                process_data,
                'interval',
                minutes=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'hourly':
            scheduler.add_job(
                process_data,
                'interval',
                hours=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'daily':
            hour, minute = map(int, start_time.split(':'))
            scheduler.add_job(
                process_data,
                'cron',
                hour=hour,
                minute=minute,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'weekly':
            scheduler.add_job(
                process_data,
                'interval',
                weeks=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type.endswith('h'):
            scheduler.add_job(
                process_data,
                'interval',
                hours=int(schedule_type[:-1]),
                args=[agent_id],
                id=job_id
            )
        else:
            logger.error(f"Unsupported schedule type: {schedule_type}")
            return

        logger.info(f"Scheduled {schedule_type} processing for {agent_id}")
        process_data(agent_id)  # Immediate first run

    except Exception as e:
        logger.error(f"Scheduling failed for {agent_id}: {e}")

def process_data(agent_id: str):
    logger.info(f"Starting data processing for {agent_id}")
    
    agent_config = fetch_agent_config(agent_id)
    if not agent_config:
        logger.error(f"Configuration not found for {agent_id}")
        return

    log_path = agent_config.get('log_path')
    map_path = agent_config.get('map_path')
    output_file = agent_config.get('output_file', 'amazon_interactions.json')

    try:
        # Read and parse files
        log_content = read_azure_file(log_path)
        mapping_content = read_azure_file(map_path)
        
        if log_path.endswith(".json"):
            log_data = json.loads(log_content)
        elif log_path.endswith(".csv"):
            log_data = list(csv.DictReader(io.StringIO(log_content)))
        else:
            raise ValueError("Unsupported file format")
        
        mapping_data = json.loads(mapping_content)

        # Process data
        transformed = transform_log(log_data, mapping_data)
        interactions = create_interactions(transformed)

        # Write output
        write_azure_file(output_file, json.dumps(interactions))

        # Redis integration
        redis_client = redis.Redis(host='redis', port=6379, db=0)
        for interaction in interactions:
            redis_client.lpush("interactions_queue", json.dumps(interaction))

        # RabbitMQ notification
        try:
            credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=Config.RABBITMQ_HOST,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)        
            channel = connection.channel()

            channel.queue_declare(queue='message_queue', durable=True)
            channel.basic_publish(
                exchange='',
                routing_key='message_queue',
                body='Interactions ready',
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    expiration='600000'
                )
            )
            connection.close()
            logger.info(f"✅ Sent ready message for {agent_id}")
        except Exception as e:
            logger.error(f"❌ Failed to send ready message: {e}")

        logger.info(f"Processed {len(interactions)} interactions for {agent_id}")

    except Exception as e:
        logger.error(f"Processing failed for {agent_id}: {e}")

def listen_for_agent_events():
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        credentials=credentials
    )
    
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue='agent_events', durable=True)

            def callback(ch, method, properties, body):
                try:
                    event = json.loads(body)
                    agent_id = event.get('agent_id')
                    action = event.get('action')
                    
                    if action in ['create', 'update']:
                        logger.info(f"Processing {action} for {agent_id}")
                        schedule_agent_processing(agent_id)
                    elif action == 'delete':
                        job_id = f"agent_{agent_id}"
                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)
                            logger.info(f"Removed job for {agent_id}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

            channel.basic_consume(
                queue='agent_events',
                on_message_callback=callback,
                auto_ack=True
            )
            logger.info("Listening for agent events...")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"RabbitMQ connection error: {e}, retrying in 5s...")
            time.sleep(5)

def main():
    wait_for_rabbitmq("rabbitmq", 5672)
    logger.info("Starting multi-tenant preprocessor service")
    
    # Start scheduler
    scheduler.start()
    
    # Start listening for agent events
    listen_for_agent_events()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logger.info("Service stopped gracefully")

if __name__ == "__main__":
    main()