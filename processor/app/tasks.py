import os
import openai
import json
import pandas as pd
from celery import Celery
from kombu import Exchange, Queue
from processor.app.topic_extraction import Config, fetch_saved_topics, process_batch, read_azure_file, fetch_saved_queries
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# Configure Celery app with broker and backend.
app = Celery('tasks', broker='amqp://myuser:mypassword@rabbitmq:5672//', backend="rpc://")

# Custom queue and exchange names
MAIN_QUEUE = "main_queue"
DLQ_QUEUE = "custom_dlq"
MAIN_EXCHANGE = Exchange("main_exchange", type="direct", durable=True)
DLX_EXCHANGE = Exchange("dlx_exchange", type="direct", durable=True)

# Configure Celery queues and DLQ settings
app.conf.update(
    task_queues=[
        Queue(
            MAIN_QUEUE,
            exchange=MAIN_EXCHANGE,
            routing_key=MAIN_QUEUE,
            queue_arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE.name,
                "x-dead-letter-routing-key": DLQ_QUEUE,
            },
        ),
        Queue(
            DLQ_QUEUE,
            exchange=DLX_EXCHANGE,
            routing_key=DLQ_QUEUE,
        ),
    ],
    task_default_queue=MAIN_QUEUE,
    task_acks_late=True,              # Acknowledge after task completes
    task_reject_on_worker_lost=True,  # Reject on worker failure
    task_acks_on_failure_or_timeout=False,  # Don't ack on failure
)

def send_email_notification(error_message, task_id):
    """
    Simulates sending an email notification for specific errors.
    """
    print(f"Simulated Email for Missing API: Task {task_id} encountered an error: {error_message}")

def send_json_notification(error_message, task_id):
    """
    Simulates sending an email notification for JSON errors.
    """
    print(f"Simulated Email for JSON: Task {task_id} encountered an error: {error_message}")

@app.task(
    name="tasks.process_topic_batch",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 0},
)
def process_topic_batch(self, batch_ids):
    """
    Process a batch of interactions for topic extraction.
    - For main queue tasks: Only route errors to DLQ.
    - For DLQ tasks: Handle errors (simulate email notifications) after fetching the full data.
    
    Here, batch_ids is a list of record IDs. The task will:
      1. Read the full dataset from Azure File Storage.
      2. Filter out records matching the provided IDs.
      3. Process the filtered interactions.
    """
    is_dlq = (
        self.request.delivery_info.get("routing_key") == DLQ_QUEUE
        if self.request.delivery_info else False
    )
    try:

        import time
        # Wait for 5 seconds to allow the file to be generated/freshened
        time.sleep(2)
        # Load full interaction data from Azure
        input_content = read_azure_file(Config.INPUT_JSON)
        input_content_saved_queries = read_azure_file(Config.QUERY_FILE)

        full_data = json.loads(input_content)
        df_full = pd.DataFrame(full_data)
        
        # Filter interactions using the provided list of IDs
        df = df_full[df_full["interaction_id"].isin(batch_ids)]
        if df.empty:
            raise ValueError("No matching records found for provided IDs")
        
        # Initialize OpenAI client (simulate error if API key missing)
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Invalid JSON Parsing")  # or appropriate error message
        openai.api_key = api_key
        client = openai
        # Process the filtered batch
        existing_topics = fetch_saved_topics()  # Assumes this function is defined
        existing_queries = fetch_saved_queries()
        result = process_batch(client, df, existing_topics, existing_queries)  # Assumes process_batch is defined
        print(f"Processed batch {self.request.id} successfully.")
        
        return result

    except Exception as e:
        error_message = str(e)
        if is_dlq:
            # In DLQ tasks, simulate sending notifications for specific errors.
            if "Missing OPENAI_API_KEY" in error_message or "Openai Balance" in error_message:
                send_email_notification(error_message, self.request.id)
            elif "No JSON array found in the response." in error_message:
                send_json_notification(error_message, self.request.id)
            
            print(f"DLQ Handling: Task {self.request.id} failed with error: {error_message}")
            return {
                "status": "failed",
                "error": error_message,
                "task_id": self.request.id,
                "batch_ids": batch_ids,
            }
        else:
            # For main queue tasks, simply route the error to DLQ (no notifications)
            if "No JSON array found in the response." in error_message:
                send_json_notification(error_message, self.request.id)
            print(f"Main queue error: {error_message} - Routing to DLQ")
            self.request.requeue = False  # Ensure it goes to DLQ.
            raise
