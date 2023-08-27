# storage.py
import psycopg2
from kafka import KafkaConsumer

# Initialize Kafka consumer
consumer = KafkaConsumer("data_engineer_job_topic", bootstrap_servers='localhost:9092')

# Set up PostgreSQL connection
conn = psycopg2.connect(
    database="your_database",
    user="your_user",
    password="your_password",
    host="localhost"
)
cursor = conn.cursor()

for message in consumer:
    job_data = message.value
    job_title = job_data["job_title"]
    company_name = job_data["company_name"]
    location = job_data["location"]
    
    insert_query = "INSERT INTO data_engineer_jobs (job_title, company_name, location) VALUES (%s, %s, %s)"
    data_tuple = (job_title, company_name, location)
    cursor.execute(insert_query, data_tuple)
    conn.commit()

# Close connections
conn.close()
