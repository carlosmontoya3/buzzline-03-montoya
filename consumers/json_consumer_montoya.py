"""
json_consumer_case.py

Consume JSON messages from a Kafka topic and process them.

Example JSON message (after deserialization):
{
    "Rank": "1",
    "Bank Name": "Industrial and Commercial Bank of China",
    "Total Assets (2023, US$ billion)": "6,303.44"
}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict
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
    topic = "banks_data"  # Ensure this matches the actual Kafka topic name
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "banks_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold bank data
#####################################

# Dictionary to store bank data {Rank: (Bank Name, Total Assets)}
bank_data = {}


#####################################
# Function to process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message received: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict = json.loads(message)

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            rank = message_dict.get("Rank", "Unknown Rank")
            bank_name = message_dict.get("Bank Name", "Unknown Bank")
            total_assets_str = message_dict.get("Total Assets (2023, US$ billion)", "0")

            # Convert assets to float (handling commas in numbers)
            try:
                total_assets = float(total_assets_str.replace(",", ""))
            except ValueError:
                logger.warning(f"Invalid asset value for bank {bank_name}: {total_assets_str}")
                total_assets = 0.0

            # Store in dictionary
            bank_data[rank] = (bank_name, total_assets)

            logger.info(f"Processed Bank: {bank_name}, Rank: {rank}, Assets: ${total_assets}B")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages containing bank financial data.
    """
    logger.info("START consumer.")

    # Fetch Kafka topic and group ID
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    # Log final stored data
    logger.info(f"Final processed bank data: {json.dumps(bank_data, indent=2)}")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()