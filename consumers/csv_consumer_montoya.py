"""
csv_consumer_montoya.py

Consume JSON messages from a Kafka topic and process them.

Example Kafka message format:
{
    "timestamp": "2025-02-10T18:15:00Z",
    "rank": "1",
    "bank_name": "Industrial and Commercial Bank of China",
    "total_assets": 6303.44
}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BANKS_TOPIC", "banks_data")  # Ensure correct topic name
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BANKS_CONSUMER_GROUP_ID", "banks_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("BANKS_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Function to process a single message
#####################################


def process_message(message: str, rolling_window: deque) -> None:
    """
    Process a JSON-transferred CSV message.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of latest bank data.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message received: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)

        # Extract fields
        timestamp = data.get("timestamp")
        rank = data.get("rank")
        bank_name = data.get("bank_name", "Unknown Bank")
        total_assets = data.get("total_assets")

        # Validate required fields
        if not all([timestamp, rank, bank_name, total_assets]):
            logger.error(f"Invalid message format: {message}")
            return

        # Store the latest bank data in the rolling window
        rolling_window.append({
            "timestamp": timestamp,
            "rank": rank,
            "bank_name": bank_name,
            "total_assets": total_assets
        })

        # Log processed message
        logger.info(
            f"Processed Bank Data - Rank: {rank}, Name: {bank_name}, Assets: ${total_assets}B, Timestamp: {timestamp}"
        )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START Banks Data Consumer.")

    # Fetch Kafka topic and group ID
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    # Rolling window stores recent bank data messages
    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
