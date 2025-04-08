import logging
import os
import json
import re
from typing import List, Dict, Set

import openai
import pandas as pd
from openai import OpenAI
from azure.storage.fileshare import ShareServiceClient
from statistics import mean
from dotenv import load_dotenv
load_dotenv()


# Configuration
class Config:
    """Application configuration constants"""
    INPUT_JSON = "amazon_interactions.json"  # updated file name
    TOPIC_FILE = "distinct_topics.txt"
    QUERY_FILE = "distinct_queries.txt"
    OUTPUT_JSON = "final_topics.json"
    BATCH_SIZE = 10
    MODEL_NAME = "gpt-4o-mini"
    TEMPERATURE = 0.0
    REQUIRED_COLUMNS = {"interaction_id", "interactions"}
    OUTPUT_COLUMNS = ["interaction_id", "Topic", "Error_Value", "Error_Category"]


    # Azure File Storage settings (set these in your environment)
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_FILE_SHARE_NAME = os.getenv("AZURE_FILE_SHARE_NAME")
    AZURE_FILE_DIRECTORY = os.getenv("AZURE_FILE_DIRECTORY", "")  # Use root by default

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("topic_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Azure File Storage helper functions
def get_directory_client():
    """Get the Azure File Storage directory client."""
    print(f"Connection String: {Config.AZURE_STORAGE_CONNECTION_STRING}")
    service_client = ShareServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    share_client = service_client.get_share_client(Config.AZURE_FILE_SHARE_NAME)
    return share_client.get_directory_client(Config.AZURE_FILE_DIRECTORY)

def read_azure_file(file_name: str) -> str:
    """
    Read the content of a file from Azure File Storage.
    
    Args:
        file_name: Name/path of the file in the share.
    
    Returns:
        File content as a string.
    """
    directory_client = get_directory_client()
    file_client = directory_client.get_file_client(file_name)
    download_stream = file_client.download_file()
    return download_stream.readall().decode('utf-8')

def write_azure_file(file_name: str, content: str) -> None:
    """
    Write content to a file in Azure File Storage.

    Args:
        file_name: Name/path of the file in the share.
        content: Content to write as a string.
    """
    directory_client = get_directory_client()
    file_client = directory_client.get_file_client(file_name)

    try:
        file_client.delete_file()  # Ensure overwrite by deleting the existing file first
    except Exception as e:
        logger.warning(f"File {file_name} does not exist or could not be deleted: {e}")

    file_client.upload_file(content.encode('utf-8'))


# Core functions
def validate_input_data(df: pd.DataFrame) -> None:
    """
    Validate input DataFrame structure and content.
    
    Args:
        df: Input DataFrame to validate
        
    Raises:
        ValueError: If validation fails.
    """
    missing_columns = Config.REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    
    if df["id"].duplicated().any():
        raise ValueError("Duplicate IDs found in input data")

def fetch_saved_topics(file_path: str = Config.TOPIC_FILE) -> Set[str]:
    """
    Retrieve existing topics from Azure File Storage.
    
    Args:
        file_path: File name in Azure File Storage.
        
    Returns:
        Set of existing topics.
    """
    try:
        content = read_azure_file(file_path)
        return {line.strip() for line in content.splitlines() if line.strip()}
    except Exception as e:
        logger.warning(f"Topic file not found or error reading file: {e}. Starting with empty taxonomy.")
        return set()

def save_new_topics(topics: Set[str], file_path: str = Config.TOPIC_FILE) -> None:
    """
    (Disabled) Persist new topics to storage file.
    """
    logger.info("New topic saving is disabled. Predefined topic list remains fixed.")

def fetch_saved_queries(file_path: str = Config.QUERY_FILE) -> Set[str]:
    try:
        content = read_azure_file(file_path)
        return {line.strip() for line in content.splitlines() if line.strip()}
    except Exception as e:
        logger.warning(f"Query file not found or error reading file: {e}. Starting with empty set.")
        return set()

# Save new queries to Azure File Storage
def save_new_queries(queries: Set[str], file_path: str = Config.QUERY_FILE) -> None:
    try:
        existing = fetch_saved_queries(file_path)
        all_queries = existing.union(queries)
        content = "\n".join(sorted(all_queries))
        write_azure_file(file_path, content)
        logger.info(f"Saved {len(queries)} new queries to {file_path}")
    except Exception as e:
        logger.error(f"Failed to save queries: {e}")

def validate_api_response(response_data: List[Dict], batch_ids: Set[str]) -> bool:
    """
    Validate structure and content of OpenAI API response.
    
    Args:
        response_data: Parsed response data.
        batch_ids: Expected interaction IDs.
        
    Returns:
        True if valid, False otherwise.
    """
    if not isinstance(response_data, list):
        logger.error("Invalid response format: Expected list")
        return False
    
    required_keys = {"interaction_id", "Topic", "Error_Value", "Error_Category"}
    
    for item in response_data:
        if not isinstance(item, dict):
            logger.error("Invalid item format: Expected dict")
            return False
            
        missing_keys = required_keys - set(item.keys())
        if missing_keys:
            logger.error(f"Missing required keys {missing_keys} in item: {item}")
            return False
            
        if str(item["interaction_id"]) not in batch_ids:
            logger.error(f"Invalid interaction ID: {item['interaction_id']}")
            return False
            
        if not isinstance(item["Error_Value"], int):
            logger.error(f"Invalid Error_Value type in item: {item}")
            return False
            
    return True

def validate_api_response_rb(response_data_rb: List[Dict], batch_ids: Set[str]) -> bool:
    """
    Validate structure and content of OpenAI API response.
    
    Args:
        response_data: Parsed response data.
        batch_ids: Expected interaction IDs.
        
    Returns:
        True if valid, False otherwise.
    """
    if not isinstance(response_data_rb, list):
        logger.error("Invalid response format: Expected list")
        return False
    
    required_keys = {"interaction_id", "Risky_Behaviour", "Top_Issues", "Top_Queries", "Issue"}
    
    for item in response_data_rb:
        if not isinstance(item, dict):
            logger.error("Invalid item format: Expected dict")
            return False
            
        missing_keys = required_keys - set(item.keys())
        if missing_keys:
            logger.error(f"Missing required keys {missing_keys} in item: {item}")
            return False
            
        if str(item["interaction_id"]) not in batch_ids:
            logger.error(f"Invalid interaction ID: {item['interaction_id']}")
            return False
            
    return True

def validate_api_response_int(response_data_int: List[Dict], batch_ids: Set[str]) -> bool:
    """
    Validate structure and content of OpenAI API response.
    
    Args:
        response_data: Parsed response data.
        batch_ids: Expected interaction IDs.
        
    Returns:
        True if valid, False otherwise.
    """
    if not isinstance(response_data_int, list):
        logger.error("Invalid response format: Expected list")
        return False
    
    required_keys = {"interaction_id", "Intent", "Sentiment", "Emotion"}
    
    for item in response_data_int:
        if not isinstance(item, dict):
            logger.error("Invalid item format: Expected dict")
            return False
            
        missing_keys = required_keys - set(item.keys())
        if missing_keys:
            logger.error(f"Missing required keys {missing_keys} in item: {item}")
            return False
            
        # if str(item["interaction_id"]) not in batch_ids:
        #     logger.error(f"Invalid interaction ID: {item['interaction_id']}")
        #     return False
            
    return True

import json
import requests
import time
import ast
import re
import json
import dateutil.parser
from collections import defaultdict, Counter
from pymongo import MongoClient

def process_batch(client, batch, existing_topics: set, saved_queries: set):
    """
    Process a batch of interactions through OpenAI API and return a combined JSON-serializable result
    with both interaction-level and session-level metrics.
    Includes new metrics for Geometric Location and Resolution.
    """

    from collections import defaultdict
    batch_records = batch.to_dict(orient="records")
    id_to_record = {str(rec["interaction_id"]): rec for rec in batch_records}
    
# Group batch records by session_id and combine interactions for each session.
    session_prompt_data = []
    sessions = defaultdict(list)
    for rec in batch_records:
        sessions[rec["session_id"]].append(rec["interactions"])

    for session_id, interactions_list in sessions.items():
        # Combine interactions with a newline delimiter for clarity.
        combined_interactions = "\n".join(interactions_list)
        session_prompt_data.append({
            "session_id": session_id,
            "interactions": combined_interactions
        })

    # Extract unique IP addresses from the batch
    unique_ips = {rec.get("ip_address") for rec in batch_records if rec.get("ip_address")}
    ip_location_map = {}
    for ip in unique_ips:
        url = f"http://ip-api.com/json/{ip}"
        try:
            response = requests.get(url, timeout=5)
            if not response.text.strip():
                raise ValueError("Empty response from server")
            ip_data = response.json()
            if ip_data.get("status") == "success":
                ip_location_map[ip] = {
                    "country": ip_data.get("country", "N/A"),
                    "state": ip_data.get("regionName", "N/A"),
                    "city": ip_data.get("city", "N/A")
                }
            else:
                ip_location_map[ip] = {"country": "N/A", "state": "N/A", "city": "N/A"}
        except Exception as e:
            ip_location_map[ip] = {"country": "N/A", "state": "N/A", "city": "N/A"}
        time.sleep(1)  # Delay to avoid rate limits
    
    # Update each record with the location data
    for rec in batch_records:
        ip = rec.get("ip_address")
        rec["location"] = ip_location_map.get(ip, {"country": "N/A", "state": "N/A", "city": "N/A"})
    # Prepare prompt for API calls
    prompt = [{
        "interaction_id": str(rec["interaction_id"]),
        "chat": rec["interactions"]
    } for rec in batch_records]

    system_message = f'''
    You will be provided with a list of chatbot conversations. Each conversation is a JSON object with a pre-assigned "interaction_id" and a "chat" field. Your tasks are:

    Your tasks are:
    1. Analyze each conversation carefully.
    2. Identify the most appropriate topic from the existing ones.
    3. Evaluate potential LLM errors using the criteria below.

    Error Evaluation Criteria:
    - **1** = Inability to perform actions/transactions
    - **2** = Lack of domain-specific information
    - **3** = Misunderstanding of User Queries
    - **4** = Inability to Handle Multi-Intent Queries
    - **5** = Ambiguous Response Generation

    If Error_Value=1, specify Error_Category using the corresponding number from the list above.

    Existing Topics:
    {sorted(existing_topics)}

    Topic Selection Guidelines:
    1. First, check thoroughly if any existing topic matches.
    2. Create a NEW TOPIC only if the conversation fundamentally doesn't fit any existing ones.
    3. New topics must be generic (e.g., "Technical Support" instead of "Printer Driver Installation Issue").

    Return a list of dictionaries in this EXACT format:
    ''' + '''
    [{"interaction_id": "id1", "Topic": "Account Management", "Error_Value": 0, "Error_Category": 0},
    {"interaction_id": "id2", "Topic": "Payment Issues", "Error_Value": 1, "Error_Category": 3]

    Do not add any extra text or commentary.
    '''

    system_message_intent = f"""
    You will be provided with a list of chatbot conversations. Each conversation is a JSON object with a pre-assigned "interaction_id" and a "chat" field. Your tasks are:
    For each interaction, determine:
      - Intent: Choose from these categories:
          Inquiry, Support Request, Order & Transaction Management,
          Account & Subscription Management, Recommendation & Advice,
          Booking & Scheduling, Complaint & Concern, Acknowledgment & Gratitude, Other.
      - Sentiment: Choose from:
          Neutral, Positive, Negative, Very Positive, Very Negative.
      - Emotion: Choose from:
          Frustration, Disappointment, Thankfulness, Concern, Conclusion, Regret, Criticism, Sadness, Uncertainty.
    
    Return a JSON array **strictly** in the following format, using **only double quotes** and **only the provided interaction_ids**:
    [
        {{"interaction_id": "id1", "Intent": "Inquiry", "Sentiment": "Neutral", "Emotion": "Concern"}},
        {{"interaction_id": "id2", "Intent": "Support Request", "Sentiment": "Negative", "Emotion": "Frustration"}}
    ]

    Do not include any extra text or commentary.
    """
    
    system_message_queries = f"""
    You will be provided with a list of chatbot conversations. Each conversation is a JSON object with a pre-assigned "interaction_id" and a "chat" field. Your tasks are:

    For each interaction, determine:
      - Risky_Behaviour: a short descriptor of any risky behavior (or "None" if safe)
      - Top_Issues: a broad category if any risk is detected (or "None")
      - Issue: a specific descriptor for the issue (or "None")
      - Top_Queries: a category label for the query; first check against these existing queries:
    {sorted(saved_queries)}

    Guidelines:
      1. If no risky behavior is detected, set all fields (except interaction_id) to "None".
      2. If risky behavior is detected, provide appropriate descriptors.
    
    Return a JSON array **strictly** in the following format, using **only double quotes** and **only the provided interaction_ids**:
    [
        {{"interaction_id": "id1", "Risky_Behaviour": "Fraud Alert", "Top_Issues": "Security", "Issue": "Unauthorized Access", "Top_Queries": "Security Query"}},
        {{"interaction_id": "id2", "Risky_Behaviour": "None", "Top_Issues": "None", "Issue": "None", "Top_Queries": "General Query"}}
    ]

    Do not add any additional text or commentary.
    """

    system_message_resolution = """
    You will be provided with a list of chatbot conversation sessions. Each session is represented as a dictionary with 'session_id' and aggregated 'interactions'. Analyze each session's conversation as a whole and determine the overall Resolution, considering whether the chatbot effectively resolved the user's issue.
    The Resolution can be either Satisfied, Dissatisfied or Dropped.
    Return a JSON array of dictionaries in EXACT format:
    [
        {"session_id": "session1", "Resolution": "Satisfied"},
        {"session_id": "session2", "Resolution": "Dissatisfied"}
    ]
    """
    
    try:
        # API call for topics, errors, and resolution.
        response = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": str(prompt)}
            ]
        )
        content = response.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', content, re.DOTALL)
        if not match:
            raise ValueError("No JSON array found in the topics response.")
        json_str = match.group(0)
        response_data = json.loads(json_str)
        if not validate_api_response(response_data, {str(rec["interaction_id"]) for rec in batch_records}):
            logger.error("Invalid API response structure for topics/errors")
            return []
        
        # API call for risky behavior and queries.
        response_rb = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[
                {"role": "system", "content": system_message_queries},
                {"role": "user", "content": str(prompt)}
            ]
        )
        content_rb = response_rb.choices[0].message.content.strip()
        match_rb = re.search(r'\[.*\]', content_rb, re.DOTALL)
        if not match_rb:
            raise ValueError("No JSON array found in risky behavior response.")
        json_str_rb = match_rb.group(0)
        response_data_rb = json.loads(json_str_rb)
        if not validate_api_response_rb(response_data_rb, {str(rec["interaction_id"]) for rec in batch_records}):
            logger.error("Invalid API response structure for risky behavior")
            return []
        
        # API call for intent/sentiment/emotion.
        response_int = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[
                {"role": "system", "content": system_message_intent},
                {"role": "user", "content": str(prompt)}
            ]
        )
        content_int = response_int.choices[0].message.content.strip()
        match_int = re.search(r'\[.*\]', content_int, re.DOTALL)
        if not match_int:
            raise ValueError("No JSON array found in intent response.")
        json_str_int = match_int.group(0)
        response_data_int = json.loads(json_str_int)
        if not validate_api_response_int(response_data_int, {str(rec["interaction_id"]) for rec in batch_records}):
            logger.error("Invalid API response structure for intent metrics")
            return []
        
        response_resolution = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[
                {"role": "system", "content": system_message_resolution},
                {"role": "user", "content": str(session_prompt_data)}
            ]
        )
        content_resolution = response_resolution.choices[0].message.content.strip()
        print(content_resolution)
        logger.info(content_resolution)
        match_resolution = re.search(r'\[.*\]', content_resolution, re.DOTALL)
        if not match_resolution:
            raise ValueError("No JSON array found in resolution response.")
        json_str_resolution = match_resolution.group(0)
        response_data_resolution = json.loads(json_str_resolution)
        # Build a mapping of session_id to resolution.
        session_resolution_map = {item["session_id"]: item["Resolution"] for item in response_data_resolution}
        
        # Merge all three responses per interaction.
        combined_response = []
        
        for topic_entry in response_data:
            interaction_id = topic_entry["interaction_id"]
            rb_entry = next((rb for rb in response_data_rb if rb["interaction_id"] == interaction_id), None)
            int_entry = next((intent for intent in response_data_int if intent["interaction_id"] == interaction_id), None)
            if rb_entry and int_entry:
                original_record = id_to_record.get(interaction_id, {})
                print("Original Record below: ")
                print(original_record)
                combined_entry = {
                    **topic_entry,         # Now includes "Resolution" along with Topic, Error_Value, Error_Category
                    **rb_entry,
                    **int_entry,
                    "session_id": original_record.get("session_id"),
                    "timestamp": original_record.get("timestamp"),
                    "interactions": original_record.get("interactions"),
                    "dialog_turns": len(ast.literal_eval(original_record.get("interactions", "[]"))),
                    "location": original_record.get("location"),  # Append location info from our new metric
                    "user_id": original_record.get("user_id")
                }
                combined_response.append(combined_entry)
        
        # Group entries by session_id.
        sessions = defaultdict(list)
        for entry in combined_response:
            sessions[entry["session_id"]].append(entry)
        
        # Compute session-level metrics and merge into each interaction record.
        session_metrics_dict = {}
        for session_id, entries in sessions.items():
            # (Assuming each 'entry' here has been augmented with a 'timestamp' field as in the original code.)
            for entry in entries:
                entry["parsed_timestamp"] = dateutil.parser.parse(id_to_record[str(entry["interaction_id"])]["timestamp"])
            sorted_entries = sorted(entries, key=lambda x: x["parsed_timestamp"])
            timestamps = [entry["parsed_timestamp"] for entry in sorted_entries]
            earliest_entry = sorted_entries[0]
            latest_entry = sorted_entries[-1]
            
            # Get first and last messages from the respective interactions.
            interactions_earliest = ast.literal_eval(id_to_record[str(earliest_entry["interaction_id"])]["interactions"])
            first_message = interactions_earliest[0]["message"] if interactions_earliest else ""
            interactions_latest = ast.literal_eval(id_to_record[str(latest_entry["interaction_id"])]["interactions"])
            last_message = interactions_latest[-1]["message"] if interactions_latest else ""
            
            num_interactions = len(sorted_entries)
            engagement_level = "Low" if num_interactions <= 3 else "Medium" if num_interactions <= 7 else "High"
            sentiments = [entry["Sentiment"] for entry in sorted_entries]
            average_sentiment = Counter(sentiments).most_common(1)[0][0] if sentiments else "None"
            topics = [entry["Topic"] for entry in sorted_entries]
            dominant_topic = Counter(topics).most_common(1)[0][0] if topics else "None"
            drop_off_sentiment = latest_entry["Sentiment"]
            total_dialog_turns = sum(entry["dialog_turns"] for entry in sorted_entries)
            duration_minutes = (max(timestamps) - min(timestamps)).total_seconds() / 60.0

            # Use our new session-level resolution from the resolution agent.
            session_resolution = session_resolution_map.get(session_id, "None")
            session_location = earliest_entry.get("location", {"country": "N/A", "state": "N/A", "city": "N/A"})

            session_metrics = {
                "engagement_level": engagement_level,
                "average_user_sentiment": average_sentiment,
                "drop_off_sentiment": drop_off_sentiment,
                "dominant_topic": dominant_topic,
                "session_dialog_turns": total_dialog_turns,
                "duration_minutes": duration_minutes,
                "first_interaction": first_message,
                "last_interaction": last_message,
                "resolution": session_resolution,  # Now using session-level resolution!
                "location": session_location
            }
            session_metrics_dict[session_id] = session_metrics
        
        # Clean up temporary fields.
        for entry in combined_response:
            if "parsed_timestamp" in entry:
                del entry["parsed_timestamp"]

        # Merge session-level metrics into each interaction record.
        final_records = []
        for entry in combined_response:
            sess_metrics = session_metrics_dict.get(entry["session_id"], {})
            merged_entry = {**entry, **sess_metrics}
            final_records.append(merged_entry)

        # Now, instead of returning final_records, we’re inserting them into MongoDB.
        mongo_client = MongoClient("mongodb+srv://dbQH:kunal2001@clusterqh.pvbet.mongodb.net/")
        db = mongo_client["metrices"]  # Use your desired database name

        # Create 'interaction_metrics' collection with schema validation if it doesn't exist.
        # (Schema updated to include location and resolution fields)
        if "interaction_metrics" in db.list_collection_names():
            db.interaction_metrics.drop()

        db.create_collection(
            "interaction_metrics",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": [
                        "interaction_id", "timestamp", "Topic", "Intent", "Sentiment",
                        "Emotion", "Error_Value", "Error_Reason", "Risky_Behaviour",
                        "Top_Issues", "Top_Queries"
                    ],
                    "properties": {
                        "interaction_id": {"bsonType": "string"},
                        "timestamp": {"bsonType": "string"},
                        "Topic": {"bsonType": "string"},
                        "Intent": {"bsonType": "string"},
                        "Sentiment": {"bsonType": "string"},
                        "Emotion": {"bsonType": "string"},
                        "Error_Value": {"bsonType": "string"},
                        "Error_Reason": {"bsonType": "string"},
                        "Risky_Behaviour": {"bsonType": "string"},
                        "Top_Issues": {"bsonType": "string"},
                        "Top_Queries": {"bsonType": "string"}
                    }
                }
            }
        )

        # For conversation_metrics
        if "conversation_metrics" in db.list_collection_names():
            db.conversation_metrics.drop()

        db.create_collection(
            "conversation_metrics",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": [
                        "user_id", "timestamp", "engagement_level", "dominant_topic",
                        "avg_sentiment", "drop_off_sentiment", "error_rate", "dialog_turns",
                        "duration", "resolution", "location"
                    ],
                    "properties": {
                        "user_id": {"bsonType": "string"},
                        "timestamp": {"bsonType": "date"},
                        "engagement_level": {"bsonType": "string"},
                        "dominant_topic": {"bsonType": "string"},
                        "avg_sentiment": {"bsonType": "string"},
                        "drop_off_sentiment": {"bsonType": "string"},
                        "error_rate": {"bsonType": "int"},
                        "dialog_turns": {"bsonType": "int"},
                        "duration": {"bsonType": "int"},
                        "resolution": {"bsonType": ["string", "null"]},
                        "location": {
                            "bsonType": "object",
                            "required": ["country", "state", "city"],
                            "properties": {
                                "country": {"bsonType": "string"},
                                "state": {"bsonType": "string"},
                                "city": {"bsonType": "string"}
                            }
                        }
                    }
                }
            }
        )


        # Prepare interaction-level documents based on the Interaction Schema.
        interaction_docs = []
        for record in final_records:
            doc = {
                "interaction_id": record.get("interaction_id"),
                "timestamp": record.get("timestamp"),
                "Topic": record.get("Topic"),
                "Intent": record.get("Intent"),
                "Sentiment": record.get("Sentiment"),
                "Emotion": record.get("Emotion"),
                "Error_Value": str(record.get("Error_Value", "")),
                "Error_Reason": record.get("Error_Reason", ""),
                "Risky_Behaviour": record.get("Risky_Behaviour"),
                "Top_Issues": record.get("Top_Issues", ""),
                "Top_Queries": record.get("Top_Queries", "")
            }
            interaction_docs.append(doc)

        if interaction_docs:
            db.interaction_metrics.insert_many(interaction_docs)

        # Prepare conversation-level documents based on the Conversation Schema.
        conversation_docs = []
        for session_id, metrics in session_metrics_dict.items():
            # Get all records for the session and pick the latest timestamp.
            session_records = [rec for rec in final_records if rec["session_id"] == session_id]
            latest_record = max(session_records, key=lambda x: dateutil.parser.parse(x["timestamp"]))
            print(latest_record)
            logger.info(latest_record)
            user_id = latest_record.get("user_id", "Unknown")
            error_values = []
            for rec in session_records:
                try:
                    error_val = int(rec.get("Error_Value", "0"))
                    error_values.append(error_val)
                except Exception:
                    error_values.append(0)
            error_rate = int(round(mean(error_values))) if error_values else 0


            conv_doc = {
                "user_id": user_id,  # Adjust as necessary; now sessions are the unit for conversation metrics.
                "timestamp": dateutil.parser.parse(latest_record["timestamp"]),
                "engagement_level": metrics.get("engagement_level"),
                "dominant_topic": metrics.get("dominant_topic"),
                "avg_sentiment": metrics.get("average_user_sentiment"),
                "drop_off_sentiment": metrics.get("drop_off_sentiment"),
                "error_rate": error_rate,  # Hardcoded as per your sample schema.
                "dialog_turns": metrics.get("session_dialog_turns"),
                "duration": int(metrics.get("duration_minutes")),
                "resolution": metrics.get("resolution") if metrics.get("resolution") is not None else "",  # Note the lowercase key!
                "location": metrics.get("location")
            }
            conversation_docs.append(conv_doc)

        if conversation_docs:
            db.conversation_metrics.insert_many(conversation_docs)

        # Instead of returning final_records, return a success confirmation.
        return {"status": "success", "message": "Records inserted into MongoDB."}

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        raise ValueError(f"Error: {e}")


    
def save_analysis_results(df: pd.DataFrame, file_path: str = Config.OUTPUT_JSON) -> None:
    """
    Save analysis results to a JSON file in Azure File Storage.
    """
    try:
        json_content = df.to_json(orient="records", indent=4)
        write_azure_file(file_path, json_content)
        logger.info(f"Successfully saved {len(df)} records to {file_path} in Azure File Storage")
    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")

def consolidate_topics_in_json(client: OpenAI, input_json: str = Config.OUTPUT_JSON, output_json: str = "final_topics_consolidated.json") -> None:
    """
    Consolidate similar topics in the final JSON and save it as a new file.
    """
    try:
        content = read_azure_file(input_json)
        data = json.loads(content)
        df = pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error reading JSON: {e}")
        return

    unique_topics = set(df["Topic"].unique())
    original_topics = fetch_saved_topics()

    new_topics = unique_topics - original_topics

    print("Unique new topics:", new_topics)
    logger.info(f"Unique new topics: {new_topics}")

    if not new_topics:
        print("No new topics generated. Exiting consolidation.")
        logger.info("No new topics generated. Exiting consolidation.")
        return

    new_df = df[df["Topic"].isin(new_topics)]

    consolidation_prompt = f"""
    The following are chatbot interaction records with their new topics:
    {new_df[['interaction_id', 'Topic']].to_dict(orient='records')}
    
    Your task is to consolidate similar topics among these new topics.
    For any interactions that have similar topics, please return a JSON array where each object contains:
    "interaction_id" (as a string) and the consolidated "Topic" (e.g., "Delivery Status").
    
    Return strictly a JSON array.
    """

    try:
        response = client.chat.completions.create(
            model=Config.MODEL_NAME,
            temperature=Config.TEMPERATURE,
            messages=[
                {"role": "system", "content": "You are an expert topic consolidator."},
                {"role": "user", "content": consolidation_prompt}
            ]
        )
        content = response.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', content, re.DOTALL)
        if match:
            json_str = match.group(0)
        else:
            raise ValueError("No JSON array found in the response.")

        consolidated_data = json.loads(json_str)
        consolidated_df = pd.DataFrame(consolidated_data)
        mapping = consolidated_df.set_index("interaction_id")['Topic'].to_dict()

        df.loc[df["interaction_id"].isin(mapping.keys()), "Topic"] = df["interaction_id"].map(mapping)

        json_consolidated = df.to_json(orient="records", indent=4)
        write_azure_file(output_json, json_consolidated)
        logger.info(f"Successfully consolidated topics. New file saved as: {output_json}")
    except Exception as e:
        logger.error(f"Consolidation failed: {e}")

def analyze_interactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main analysis workflow controller.
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    if not client.api_key:
        raise ValueError("Missing OPENAI_API_KEY environment variable")

    existing_topics = fetch_saved_topics()
    saved_queries = fetch_saved_queries()

    results = []

    for i in range(0, len(df), Config.BATCH_SIZE):
        batch = df.iloc[i:i+Config.BATCH_SIZE]
        logger.info(f"Processing batch {i // Config.BATCH_SIZE + 1}/{(len(df)-1) // Config.BATCH_SIZE + 1}")
        batch_results = process_batch(
            client=client,
            batch=batch,
            existing_topics=existing_topics,
            saved_queries=saved_queries
        )
        results.extend(batch_results)

    result_df = pd.DataFrame(results)
    save_analysis_results(result_df)
    consolidate_topics_in_json(client, input_json=Config.OUTPUT_JSON)
    return result_df

if __name__ == "__main__":
    try:
        # Read the input JSON file from Azure File Storage
        input_data = json.loads(read_azure_file(Config.INPUT_JSON))
        input_df = pd.DataFrame(input_data)
        # input_df["id"] = input_df.index.astype(str)
        analysis_results = analyze_interactions(input_df)
        logger.info("Analysis pipeline completed successfully")
    except Exception as e:
        logger.error(f"Analysis pipeline failed: {str(e)}", exc_info=True)
        raise
