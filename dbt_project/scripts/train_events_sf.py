from confluent_kafka import Consumer
import snowflake.connector
from datetime import datetime
import json

# Kafka Consumer Configuration
kafka_config = {
    'bootstrap.servers': 'id.us-east-1.aws.confluent.cloud',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'auto.offset.reset': 'earliest',
    'sasl.username': 'kafka_key',
    'sasl.password': 'token',
    'linger.ms': 50,
    'group.id': 'python-consumer-group',
    'batch.num.messages': 1000,
    'compression.type': 'lz4',
    'message.timeout.ms': 30000
}

# Snowflake Configuration
snowflake_config = {
    "account": '',
    "user": '',
    "password": '',
    "role": "",
    "warehouse": '',
    "database": '',
    "schema": ''
}

# Initialize Kafka Consumer
consumer = Consumer(kafka_config)
consumer.subscribe(['train-movements'])  # Replace with your Kafka topic

# Establish Snowflake Connection
sf_conn = snowflake.connector.connect(**snowflake_config)
cursor = sf_conn.cursor()

# Batch configuration
BATCH_SIZE = 500  # Number of records to insert in bulk
batch_data = []  # Buffer to hold batch records


def convert_unix_to_iso(unix_timestamp):
    """Convert Unix timestamp to ISO format."""
    if unix_timestamp is None or unix_timestamp == "" or unix_timestamp == 0:
        return None  # Return None for NULL values in Snowflake
    return datetime.utcfromtimestamp(int(unix_timestamp) / 1000).strftime('%Y-%m-%d %H:%M:%S')


def process_message(data):
    """Process a single Kafka message and prepare it for insertion."""
    # Define all expected keys in the JSON data
    expected_keys = [
        'train_id', 'actual_timestamp', 'loc_stanox', 'gbtt_timestamp', 'planned_timestamp',
        'planned_event_type', 'event_type', 'event_source', 'correction_ind', 'offroute_ind',
        'direction_ind', 'line_ind', 'platform', 'route', 'train_service_code',
        'division_code', 'toc_id', 'timetable_variation', 'variation_status',
        'next_report_stanox', 'next_report_run_time', 'train_terminated',
        'delay_monitoring_point', 'reporting_stanox', 'auto_expected'
    ]

    # Ensure all keys exist in the dictionary, assign None if missing
    for key in expected_keys:
        if key not in data or data[key] is None:
            data[key] = None

    # Prepare values for insertion, handling missing or empty data
    return (
        data.get('train_id'),
        convert_unix_to_iso(data.get('actual_timestamp')),
        data.get('loc_stanox'),
        convert_unix_to_iso(data.get('gbtt_timestamp')),
        convert_unix_to_iso(data.get('planned_timestamp')),
        data.get('planned_event_type'),
        data.get('event_type'),
        data.get('event_source'),
        data.get('correction_ind'),
        data.get('offroute_ind'),
        data.get('direction_ind'),
        data.get('line_ind'),
        data.get('platform'),
        data.get('route'),
        data.get('train_service_code'),
        data.get('division_code'),
        data.get('toc_id'),
        data.get('timetable_variation'),
        data.get('variation_status'),
        data.get('next_report_stanox'),
        data.get('next_report_run_time'),
        data.get('train_terminated'),
        data.get('delay_monitoring_point'),
        data.get('reporting_stanox'),
        data.get('auto_expected')
    )


def bulk_insert(batch):
    """Perform a bulk insert into Snowflake."""
    if not batch:
        return

    # SQL Query for bulk insertion
    insert_query = """
    INSERT INTO TRAIN_MOVEMENT (
        TRAIN_ID, ACTUAL_TIMESTAMP, LOC_STANOX, GBTT_TIMESTAMP, PLANNED_TIMESTAMP,
        PLANNED_EVENT_TYPE, EVENT_TYPE, EVENT_SOURCE, CORRECTION_IND, OFFROUTE_IND,
        DIRECTION_IND, LINE_IND, PLATFORM, ROUTE, TRAIN_SERVICE_CODE,
        DIVISION_CODE, TOC_ID, TIMETABLE_VARIATION, VARIATION_STATUS,
        NEXT_REPORT_STANOX, NEXT_REPORT_RUN_TIME, TRAIN_TERMINATED,
        DELAY_MONITORING_POINT, REPORTING_STANOX, AUTO_EXPECTED, LAST_UPDATED_DT
    ) VALUES (
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      CURRENT_TIMESTAMP()
    )
    """
    
    cursor.executemany(insert_query, batch)
    sf_conn.commit()  # Commit the transaction


try:
    while True:
        # Poll messages from Kafka
        msg = consumer.poll(1.0)  # Timeout of 1 second

        if msg is None:
            continue  # No message received; continue polling

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Extract message value and process it
        raw_data = msg.value().decode('utf-8')  # Assuming UTF-8 encoding
        json_data = json.loads(raw_data)
        
        processed_record = process_message(json_data)
        
        # Add processed record to the batch buffer
        batch_data.append(processed_record)

        # If batch size is reached or exceeded, perform a bulk insert
        if len(batch_data) >= BATCH_SIZE:
            bulk_insert(batch_data)
            print(f"Inserted {len(batch_data)} records into Snowflake.")
            batch_data.clear()  # Clear the buffer after insertion

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    # Insert any remaining records in the buffer before shutting down
    if batch_data:
        bulk_insert(batch_data)
    
    # Cleanup resources
    consumer.close()
    cursor.close()
    sf_conn.close()
