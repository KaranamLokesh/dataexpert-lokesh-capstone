import os
import time
import json
import logging
from stomp import Connection, ConnectionListener
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server
from threading import Lock

# Configuration constants
NR_USERNAME = "abc@gmail.com"
NR_PASSWORD = ""  # Must meet complexity requirements [3][7]
CLIENT_ID = "fantastic-capstone-lokesh"  # Unique identifier

KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'kafka_key',
    'sasl.password': 'token',
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'compression.type': 'lz4',
    'message.timeout.ms': 30000
}

STOMP_CONFIG = [('publicdatafeeds.networkrail.co.uk', 61618)]
KAFKA_TOPIC = 'train-movements'

# Metrics definitions
MESSAGES_RECEIVED = Counter(
    'stomp_messages_received_total',
    'Total messages received from STOMP',
    ['topic']
)

MESSAGES_PRODUCED = Counter(
    'kafka_messages_produced_total',
    'Messages sent to Kafka',
    ['topic']
)

PROCESSING_TIME = Gauge(
    'message_processing_seconds',
    'Message processing time',
    ['topic']
)

ERROR_COUNTER = Counter(
    'processing_errors_total',  
    'Total processing errors',
    ['topic']
)

# Rate limiting constants
RATE_LIMIT_PER_SECOND = 1  # Adjust this based on API feed limits (e.g., Train Movements: 600/min)
rate_lock = Lock()
last_processed_time = time.time()
processed_count = 0


class EnhancedRailListener(ConnectionListener):
    def __init__(self, producer):
        self.producer = producer
        self._running = True
        self.metrics = {
            'received': MESSAGES_RECEIVED.labels(topic=KAFKA_TOPIC),
            'produced': MESSAGES_PRODUCED.labels(topic=KAFKA_TOPIC),
            'errors': ERROR_COUNTER.labels(topic=KAFKA_TOPIC),
            'processing': PROCESSING_TIME.labels(topic=KAFKA_TOPIC)
        }

    def on_error(self, frame):
        logging.error(f'STOMP Error: {frame.body}')
        self.metrics['errors'].inc()

    def on_message(self, frame):
        global last_processed_time, processed_count

        start_time = time.time()
        self.metrics['received'].inc()

        try:
            # Rate limiting logic
            with rate_lock:
                current_time = time.time()
                elapsed_time = current_time - last_processed_time

                if elapsed_time < 1 and processed_count >= RATE_LIMIT_PER_SECOND:
                    time.sleep(1 - elapsed_time)  # Wait until the next second window

                if elapsed_time >= 1:
                    processed_count = 0
                    last_processed_time = current_time

                processed_count += 1

            message = json.loads(frame.body)
            for event in message:
                self._process_event(event)

            self.producer.poll(0)
            self.metrics['processing'].set(time.time() - start_time)

        except Exception as e:
            self.metrics['errors'].inc()
            logging.exception(f"Failed to process message: {str(e)}")

    def _process_event(self, event):
        try:
            key = event['body']['train_id']
            value = {
                'metadata': event['header'],
                'data': event['body'],
                'processing_ts': int(time.time() * 1000),
                'source': 'network_rail'
            }
            if value['metadata'] and value['metadata']['msg_type'] == '0003':
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=json.dumps(value['data']),
                    callback=self._delivery_report
                )
                self.metrics['produced'].inc()

        except KeyError as e:
            logging.error(f"Missing key in event: {str(e)}")
            self.metrics['errors'].inc()

    def _delivery_report(self, err, msg):
        if err is not None:
            logging.error(f'Delivery failed: {err}')
            self.metrics['errors'].inc()


def main():
    # Initialize monitoring
    start_http_server(8000)
    logging.basicConfig(level=logging.INFO)

    # Create Kafka producer
    producer = Producer(KAFKA_CONFIG)

    # Configure STOMP connection
    conn = Connection([('publicdatafeeds.networkrail.co.uk', 61618)])
    listener = EnhancedRailListener(producer)

    conn.set_listener('', listener)
    # conn.connect(NR_USERNAME, NR_PASSWORD, wait=True)

    while True:
        try:
            conn.connect(NR_USERNAME, NR_PASSWORD,True,{'heart-beat': '1000000,1000000'})
            conn.subscribe(
                "/topic/TRAIN_MVT_ALL_TOC",
                'kafka-producer',
                'auto'
            )
            while listener._running:
                time.sleep(1)
        except Exception as e:
            logging.error(f"Connection error: {e}")
            time.sleep(5)  # Wait before reconnecting
        finally:
            if conn.is_connected():
                conn.disconnect()



if __name__ == "__main__":
    main()
