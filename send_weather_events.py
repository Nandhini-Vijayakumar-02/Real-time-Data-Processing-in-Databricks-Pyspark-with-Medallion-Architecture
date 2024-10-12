import time
import random
import json
from azure.eventhub import EventHubProducerClient, EventData

# Connection string and Event Hub name
connection_string = ""
eventhub_name = "streameventhub"

def generate_weather_data():
    """Generate random weather data."""
    return {
        "temperature": random.randint(-10, 40),  # Random temperature between -10 and 40
        "humidity": random.randint(0, 100),       # Random humidity between 0 and 100
        "windSpeed": random.randint(0, 20),       # Random wind speed between 0 and 20
        "windDirection": random.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"]),
        "precipitation": random.choice([0, 1]),   # 0 for no precipitation, 1 for precipitation
        "conditions": random.choice(["Sunny", "Cloudy", "Partly Cloudy", "Rain", "Snow"])
    }

def main():
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(connection_string, eventhub_name=eventhub_name)

    try:
        while True:
            weather_data = generate_weather_data()
            event_data = EventData(json.dumps(weather_data))  # Convert dict to JSON string
            producer.send_event(event_data)
            print(f"Sent event: {weather_data}")
            time.sleep(5)  # Send every 5 seconds (adjust as needed)

    except Exception as e:
        print(f"Error sending events: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
