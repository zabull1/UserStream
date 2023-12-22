"""Module providing a time functions."""
import time
import json
import requests
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(1, 4, 7),
)


# Function to fetch random user data
def fetch_random_user() -> str | None:
    """fetch random user information"""

    response = requests.get("https://randomuser.me/api/", timeout=120)
    if response.status_code == 200:
        return response.json()["results"][0]
    return None


now = time.time()
# loop to simulate real-time user sign-ups
while True:
    if time.time() > now + 120:
        break
    user_data = fetch_random_user()
    if user_data:
        # Send user data to Kafka topic 'user_signups'
        producer.send("user_signups", user_data)
        print(f"Sent user data to Kafka: {user_data}")
    time.sleep(5)  # Wait for 5 seconds before generating next user
