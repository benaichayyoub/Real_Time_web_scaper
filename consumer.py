# consumer.py
from kafka import KafkaConsumer

# Initialize Kafka consumer
consumer = KafkaConsumer("data_engineer_job_topic", bootstrap_servers='localhost:9092')

for message in consumer:
    job_data = message.value
    # Process the job data, perform any transformations
    print("Received:", job_data)
