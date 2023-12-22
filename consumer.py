import json
from kafka import KafkaConsumer
import time
from dotenv import load_dotenv
import boto3
import os
import uuid


consumer = KafkaConsumer(
    "users",
    bootstrap_servers=["broker:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer.group.id.demo.2",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

load_dotenv()

aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
region_name = os.environ["AWS_REGION"]
aws_bucket = os.environ["S3_BUCKET_NAME"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
)

# Poll for new messages for a specific duration
timeout_seconds = 120
start_time = time.time()

while True:
    try:
    
        message = next(consumer)

        # Process the received message
        print(f"Received message: {message.value}")

        user_signons = str(uuid.uuid4())
        object_key = f"user_signons_/{user_signons}.json"

        # Convert the message value to a JSON string
        message_json = json.dumps(message.value)

        # Write the JSON string to the S3 bucket
        s3.put_object(Body=message_json, Bucket=aws_bucket, Key=object_key)

        print(f"Uploaded file to s3://{aws_bucket}/{object_key}")

        if time.time() > start_time + timeout_seconds:
            # Break the loop after the specified timeout
            print("Timeout reached. Exiting.")
            break

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        print("Consumer closed.")
