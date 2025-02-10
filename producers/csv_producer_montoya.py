"""
csv_producer_montoya.py

Stream bank financial data to a Kafka topic.

It is common to transfer CSV data as JSON so 
each field is clearly labeled.
"""

#####################################
# Import Modules
#####################################

import os
import sys
import time  # control message intervals
import pathlib  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
from datetime import datetime  # work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
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


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BANKS_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE = DATA_FOLDER.joinpath("banks.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        str: JSON-formatted message with bank financial data.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as csv_file:
                logger.info(f"Reading bank data from file: {file_path}")

                csv_reader = csv.DictReader(csv_file)

                # Ensure the required columns exist
                required_columns = {"Rank", "Bank Name", "Total Assets (2023, US$ billion)"}
                if not required_columns.issubset(set(csv_reader.fieldnames)):
                    logger.error(f"Missing required columns in CSV: {csv_reader.fieldnames}")
                    sys.exit(1)

                for row in csv_reader:
                    try:
                        # Extract and clean up data
                        rank = row.get("Rank", "").strip()
                        bank_name = row.get("Bank Name", "Unknown Bank").strip()
                        total_assets_str = row.get("Total Assets (2023, US$ billion)", "0").strip()

                        # Convert total assets to float (handling commas)
                        total_assets = float(total_assets_str.replace(",", ""))

                        # Generate timestamp and prepare the message
                        current_timestamp = datetime.utcnow().isoformat()
                        message = {
                            "timestamp": current_timestamp,
                            "rank": rank,
                            "bank_name": bank_name,
                            "total_assets": total_assets,
                        }
                        logger.debug(f"Generated message: {message}")
                        yield message

                    except ValueError as ve:
                        logger.warning(f"Skipping invalid row due to ValueError: {row} - {ve}")
                        continue
                    except Exception as e:
                        logger.error(f"Unexpected error processing row {row}: {e}")
                        continue

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Define main function for this module.
#####################################


def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("START bank data producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END bank producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
