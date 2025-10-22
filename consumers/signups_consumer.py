import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

# -----------------------
# CONFIGURATION
# -----------------------
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'signups_topic'

DB_HOST = 'localhost'
DB_NAME = 'churn_db'
DB_USER = 'admin'
DB_PASS = 'admin'

# -----------------------
# CONNECT TO POSTGRES
# -----------------------
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()

# -----------------------
# SETUP KAFKA CONSUMER
# -----------------------
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'signups-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TOPIC])

print("Signups consumer started, listening to Kafka...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Parse message
        data = json.loads(msg.value().decode('utf-8'))

        # Insert into Postgres
        cur.execute("""
            INSERT INTO customers (customer_id, email, signup_date, region, plan, device)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
        """, (
            data['customer_id'],
            data['email'],
            data['signup_date'],
            data['region'],
            data['plan'],
            data['device']
        ))
        conn.commit()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    cur.close()
    conn.close()
