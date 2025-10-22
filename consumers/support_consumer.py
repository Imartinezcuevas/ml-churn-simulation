import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

# -----------------------
# CONFIGURATION
# -----------------------
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'support_topic'

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
    'group.id': 'support-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TOPIC])

print("Support consumer started, listening to Kafka...")

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
            INSERT INTO support (customer_id, ticket_date, issue_type, resolved)
            VALUES (%s, %s, %s, %s)
        """, (
            data['customer_id'],
            data['ticket_date'],
            data['issue_type'],
            data['resolved']
        ))
        conn.commit()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    cur.close()
    conn.close()
