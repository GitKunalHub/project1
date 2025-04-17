import os
import json
import csv
import io
import logging
import sys
import time
import socket
from urllib.parse import quote_plus
from typing import Any, Dict, List, Union
from bson import ObjectId  # Added to convert string ids to ObjectId

# Third-party libraries
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import pymongo
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
import redis
import pika

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
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
    AZURE_BLOB_CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME")
    # MongoDB
    MONGO_USER = quote_plus(os.getenv("MONGODB_USER"))
    MONGO_PASS = quote_plus(os.getenv("MONGODB_PASS"))
    MONGO_HOST = os.getenv("MONGODB_HOST")
    MONGO_PORT = os.getenv("MONGODB_PORT")
    MONGO_AUTH_SOURCE = quote_plus(os.getenv("MONGODB_AUTH_SOURCE"))
    # Update MongoDB connection and collection parameters
    MONGODB_URI = (
        f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    )
    MONGODB_DB = os.getenv("MONGODB_DB")
    # Update collection name to the new one
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

    # RabbitMQ
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER")
    RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
    RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")

# Update scheduler jobstore to use new collection name if needed.
scheduler = BackgroundScheduler(
    jobstores={
        'default': MongoDBJobStore(
            database=Config.MONGODB_DB,
            collection='scheduler_jobs',
            client=pymongo.MongoClient(Config.MONGODB_URI)
        )
    },
    timezone='UTC'
)

def get_blob_client(blob_name: str):
    blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(Config.AZURE_BLOB_CONTAINER_NAME)
    return container_client.get_blob_client(blob_name)

def read_azure_file(blob_name: str) -> str:
    blob_client = get_blob_client(blob_name)
    download_stream = blob_client.download_blob()
    logger.info(f"Reading file '{blob_name}' from Azure Blob Storage.")
    return download_stream.readall().decode('utf-8')

def write_azure_file(blob_name: str, content: str) -> None:
    blob_client = get_blob_client(blob_name)
    logger.info(f"Writing file '{blob_name}' to Azure Blob Storage.")
    try:
        blob_client.delete_blob()
        logger.info(f"Deleted existing blob '{blob_name}'.")
    except Exception:
        logger.info(f"Blob '{blob_name}' does not exist, proceeding to upload.")
        pass  # Blob may not exist
    blob_client.upload_blob(content.encode('utf-8'), overwrite=True)

def wait_for_rabbitmq(host, port, timeout=30):
    """Wait until RabbitMQ is available."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=2):
                print("RabbitMQ is up!")
                return
        except Exception as e:
            if time.time() - start_time > timeout:
                print(f"Timeout waiting for RabbitMQ! host: {host}, port: {port}")
                raise
            print(f"Waiting for RabbitMQ... host: {host}, port: {port}")
            time.sleep(2)

def fetch_agent_config(agent_id: str) -> dict:
    try:
        with pymongo.MongoClient(Config.MONGODB_URI) as client:
            db = client[Config.MONGODB_DB]
            collection = db[Config.MONGODB_COLLECTION]
            # Query by _id converted to ObjectId
            return collection.find_one({"_id": ObjectId(agent_id)}) or {}
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

def transform_log(input_data, mapping_spec):
    """
    Transforms data with nested messages (like the Amazon conversation logs).
    """
    output_data = {"conversations": []}
    
    # Normalize input_data to a list
    if isinstance(input_data, dict):
        input_data = [input_data]
    elif not isinstance(input_data, list):
        return output_data

    for conversation in input_data:
        transformed_conv = {}
        # Process all top-level fields in mapping except messages
        for out_field, spec in mapping_spec.items():
            if out_field not in ["messages", "message"]:
                transformed_conv[out_field] = apply_mapping(conversation, spec)
        
        # Determine messages: if mapping for messages is provided, use it; otherwise use the conversation itself.
        messages_path = mapping_spec.get("messages", "")
        msgs = get_nested(conversation, messages_path) if messages_path else conversation
        if not isinstance(msgs, list):
            msgs = [msgs] if msgs is not None else []
        
        transformed_messages = []
        for msg in msgs:
            transformed_msg = apply_mapping(msg, mapping_spec["message"])
            transformed_messages.append(transformed_msg)
        
        transformed_conv["messages"] = transformed_messages
        output_data["conversations"].append(transformed_conv)
    
    return output_data

def group_flat_messages(input_data, mapping_spec):
    """
    Groups flat messages by session_id for client logs.
    Each session becomes a conversation with a list of mapped messages.
    """
    sessions = {}
    for item in input_data:
        # First, apply the mapping to each flat message.
        mapped_msg = apply_mapping(item, mapping_spec["message"])
        session_id = item.get("session_id")
        user_id = item.get("user_id")
        if session_id not in sessions:
            sessions[session_id] = {
                "session_id": session_id,
                "user_id": user_id,
                "messages": []
            }
        sessions[session_id]["messages"].append(mapped_msg)
    return {"conversations": list(sessions.values())}

def create_interactions(transformed_data):
    """
    Creates interactions by pairing adjacent messages (user–bot) per session.
    """
    interactions_output = []
    for conv in transformed_data.get("conversations", []):
        session_id = conv.get("session_id")
        user_id = conv.get("user_id")
        msgs = conv.get("messages", [])
        # Pair messages by assuming each consecutive pair is a user–bot interaction.
        for i in range(0, len(msgs), 2):
            pair = msgs[i:i+2]
            # Identify the bot message from the pair (if any)
            bot_msg = None
            for m in pair:
                if m.get("sender", "").upper() == "BOT":
                    bot_msg = m
            # Fallback to first message if no bot message is found.
            if not bot_msg and pair:
                bot_msg = pair[0]
            
            simple_msgs = [{"role": m.get("sender"), "message": m.get("text")} for m in pair]
            # Use bot message id if available, else generate an id
            interaction_id = bot_msg.get("id") if bot_msg and bot_msg.get("id") else f"{session_id}_{i//2+1}"
            interactions_output.append({
                "interaction_id": interaction_id,
                "interactions": json.dumps(simple_msgs),
                "session_id": session_id,
                "timestamp": bot_msg.get("timestamp") if bot_msg else None,
                "user_id": user_id,
                "ip_address": bot_msg.get("ip_address") if bot_msg else None,
                "agent_id": bot_msg.get("bot_id") if bot_msg else None,
                "agent_name": bot_msg.get("bot_name") if bot_msg else None
            })
    return interactions_output

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
            job = scheduler.add_job(
                process_data,
                'interval',
                minutes=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'hourly':
            job = scheduler.add_job(
                process_data,
                'interval',
                hours=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'daily':
            hour, minute = map(int, start_time.split(':'))
            job = scheduler.add_job(
                process_data,
                'cron',
                hour=hour,
                minute=minute,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type == 'weekly':
            job = scheduler.add_job(
                process_data,
                'interval',
                weeks=1,
                args=[agent_id],
                id=job_id
            )
        elif schedule_type.endswith('h'):
            job = scheduler.add_job(
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
        logger.debug(f"Next run time for agent {agent_id}: {job.next_run_time}")
        process_data(agent_id)  # Immediate first run

    except Exception as e:
        logger.error(f"Scheduling failed for {agent_id}: {e}")

def process_data(agent_id: str):
    logger.info(f"Starting data processing for {agent_id}")
    
    # Fetch agent config in case it's needed further
    agent_config = fetch_agent_config(agent_id)
    if not agent_config:
        logger.error(f"Configuration not found for {agent_id}")
        return

    # For the new schema, hard-code these values
    log_path = "amazonq_conversations.json"
    map_path = "amazonq_json_mapping.json"
    output_file = "amazon_interactions.json"

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
        if isinstance(log_data, list) and log_data and "role" in log_data[0]:
            transformed_data = group_flat_messages(log_data, mapping_data)
        else:
            transformed_data = transform_log(log_data, mapping_data)

        interactions = create_interactions(transformed_data)

        # Write output
        write_azure_file(output_file, json.dumps(interactions))

        # Redis integration with agent-specific keys
        redis_client = redis.Redis(host='redis', port=6379, db=0)
        agent_queue_key = f"interactions_queue:{agent_id}"
        for interaction in interactions:
            redis_client.lpush(agent_queue_key, json.dumps(interaction))

        # RabbitMQ notification
        try:
            credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=Config.RABBITMQ_HOST,
                credentials=credentials,
                heartbeat=60
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            message_body = json.dumps({'agent_id': agent_id})
            logger.debug(f"Attempting to send message: {message_body}")
            channel.basic_publish(
                exchange='',
                routing_key='message_queue',
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    expiration='600000'
                )
            )
            logger.info(f"✅ Sent ready message for {agent_id}")
        except Exception as e:
            logger.error(f"❌ Failed to send ready message: {e}")
        finally:
            try:
                connection.close()
            except Exception as close_err:
                logger.error(f"❌ Error closing connection: {close_err}")
         
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

            def callback(ch, method, properties, body):
                try:
                    event = json.loads(body)
                    # Now, check the "message" field instead of "action"
                    event_message = event.get('message')
                    agent_id = event.get('agent_id')
                    
                    if event_message == 'AgentCreated':
                        logger.info(f"Processing AgentCreated for {agent_id}")
                        schedule_agent_processing(agent_id)
                    elif event_message == 'delete':
                        job_id = f"agent_{agent_id}"
                        if scheduler.get_job(job_id):
                            scheduler.remove_job(job_id)
                            logger.info(f"Removed job for {agent_id}")
                    else:
                        logger.info(f"Ignoring event with message: {event_message}")

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
    wait_for_rabbitmq(Config.RABBITMQ_HOST, Config.RABBITMQ_PORT)
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
