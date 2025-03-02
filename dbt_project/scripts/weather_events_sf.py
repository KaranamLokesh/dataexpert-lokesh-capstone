from confluent_kafka import Consumer
import snowflake.connector
from datetime import datetime
import json

# Kafka Consumer Configuration
kafka_config = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'auto.offset.reset': 'earliest',
    'sasl.username': '',
    'sasl.password': '',
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
consumer.subscribe(['weather-updates'])  # Replace with your Kafka topic

# Establish Snowflake Connection
sf_conn = snowflake.connector.connect(**snowflake_config)
cursor = sf_conn.cursor()

def convert_unix_to_iso(unix_timestamp):
    """Convert Unix timestamp to ISO format."""
    if unix_timestamp is None or unix_timestamp == "" or unix_timestamp == 0:
        return None  # Return None for NULL values in Snowflake
    return datetime.utcfromtimestamp(int(unix_timestamp)).strftime('%Y-%m-%d %H:%M:%S')

def upsert_weather_data_to_snowflake(data):
    """Perform an upsert operation into the weather_data table."""
    try:
        # Convert UNIX timestamps to ISO format for sunrise and sunset
        sunrise_unix = data['sys']['sunrise']
        sunset_unix = data['sys']['sunset']
        sunrise = convert_unix_to_iso(sunrise_unix)
        sunset = convert_unix_to_iso(sunset_unix)

        # Prepare the MERGE statement for upserting data
        merge_query = """
        MERGE INTO weather_data AS target
        USING (
            SELECT 
                %s AS city,
                %s AS temperature,
                %s AS humidity,
                %s AS pressure,
                %s AS visibility,
                %s AS wind_speed,
                %s AS wind_direction,
                %s AS sunrise,
                %s AS sunset,
                %s AS weather_description,
                %s AS cloudiness
        ) AS source
        ON target.city = source.city
        WHEN MATCHED THEN 
            UPDATE SET 
                temperature = source.temperature,
                humidity = source.humidity,
                pressure = source.pressure,
                visibility = source.visibility,
                wind_speed = source.wind_speed,
                wind_direction = source.wind_direction,
                sunrise = source.sunrise,
                sunset = source.sunset,
                weather_description = source.weather_description,
                cloudiness = source.cloudiness
        WHEN NOT MATCHED THEN 
            INSERT (
                city, temperature, humidity, pressure, visibility, wind_speed, 
                wind_direction, sunrise, sunset, weather_description, cloudiness
            ) VALUES (
                source.city, source.temperature, source.humidity, source.pressure, 
                source.visibility, source.wind_speed, source.wind_direction, 
                source.sunrise, source.sunset, source.weather_description, source.cloudiness
            );
        """

        # Execute the MERGE statement with the provided data
        cursor.execute(merge_query, (
            data['name'],  # city
            data['main']['temp'],  # temperature
            data['main']['humidity'],  # humidity
            data['main']['pressure'],  # pressure
            data['visibility'],  # visibility
            data['wind']['speed'],  # wind_speed
            data['wind']['deg'],  # wind_direction
            sunrise,  # sunrise time
            sunset,  # sunset time
            data['weather'][0]['description'],  # weather_description
            data['clouds']['all']  # cloudiness percentage
        ))
        
        print(f"Upserted weather data for {data['name']} into Snowflake")
    
    except Exception as e:
        print(f"Error during upsert operation: {e}")

try:
    while True:
        # Poll messages from Kafka
        msg = consumer.poll(1.0)  # Timeout of 1 second
        
        if msg is None:
            continue  # No message received; continue polling
        
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        # Extract message value and process it as JSON
        raw_data = msg.value().decode('utf-8')  # Assuming UTF-8 encoding
        json_data = json.loads(raw_data)
        
        # Perform the upsert operation for each message received from Kafka
        upsert_weather_data_to_snowflake(json_data)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    # Cleanup resources before exiting
    consumer.close()
    cursor.close()
    sf_conn.close()
