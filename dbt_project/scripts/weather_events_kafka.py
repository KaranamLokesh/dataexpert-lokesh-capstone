import json
import requests
from confluent_kafka import Producer
from time import sleep
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '',
    'sasl.password': '',
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'compression.type': 'lz4',
    'message.timeout.ms': 30000
}
# OpenWeatherMap API configuration
API_KEY = 'openweather_key'

# List of cities to fetch weather data for
CITIES = [
    'Brighton', 
    'Canterbury', 
    'Glasgow', 
    'London', 
    'Cardiff', 
    'Manchester', 
    'Cambridge', 
    'Birmingham', 
    'Newcastle', 
    'Nottingham', 
    'Bristol', 
    'Southampton', 
    'Aylesbury',
    'Liverpool',  # For HILLSIDE, FAZAKERLEY, LIVERPOOL CENTRAL
    'Leeds',      # For HEADINGLEY, HELLIFIELD
    'Sheffield'   # For TINSLEY EAST JN
]

# OpenWeatherMap API URL
URL = 'http://api.openweathermap.org/data/2.5/weather'

# Kafka configuration
KAFKA_BROKER = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092'
TOPIC = 'weather-updates'

# Kafka Producer Configuration


# Initialize the Kafka Producer
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """Delivery report callback to confirm message delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_weather(city):
    """Fetch weather data for a given city using the OpenWeatherMap API."""
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(URL, params=params)
    
    # Check if the API request was successful
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data for {city}: {response.status_code}, {response.text}")
        return None

def produce_weather_data():
    """Fetch weather data for cities and produce it to Kafka."""
    while True:
        for city in CITIES:
            try:
                # Fetch weather data from the OpenWeatherMap API
                weather_data = fetch_weather(city)
                # print(weather_data)
                if weather_data:
                    # Produce the weather data to the Kafka topic
                    producer.produce(
                        TOPIC,
                        key=city.encode('utf-8'),
                        value=json.dumps(weather_data).encode('utf-8'),
                        callback=delivery_report  # Attach the delivery report callback
                    )
                    producer.flush()  # Ensure messages are sent immediately
                    print(f"Produced weather data for {city}: {weather_data}")
            except Exception as e:
                print(f"Error producing weather data for {city}: {e}")
        
        # Wait 10 minutes before fetching new data
        sleep(600)

if __name__ == "__main__":
    print("Starting Confluent Kafka Weather Data Producer...")
    produce_weather_data()
