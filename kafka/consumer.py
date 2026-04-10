import json
import logging
import psycopg2
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
BATCH_SIZE      = 100

# FIX: Define topic names as module-level constants
TOPIC_READINGS = "weather.readings.raw"
TOPIC_ALERTS   = "weather.alerts"


def get_pg_conn():
    return psycopg2.connect(
        host="postgres", database="airflow",
        user="airflow", password="airflow"
    )


def flush_batch(batch, conn):
    if not batch:
        return
    cursor = conn.cursor()
    inserted = 0
    for r in batch:
        try:
            cursor.execute("""
                INSERT INTO weather.streaming_readings
                    (city_id, timestamp, temperature, windspeed, weather_code)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                r.get("city_id"),
                r.get("timestamp"),
                r.get("temperature"),
                r.get("windspeed"),
                r.get("weather_code"),
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Row error: {e}")
    conn.commit()
    cursor.close()
    logger.info(f"Flushed {inserted}/{len(batch)} records to PostgreSQL")


def main():
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "weather-postgres-sink",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    # FIX: Use the constant, no walrus operator
    consumer.subscribe([TOPIC_READINGS])
    logger.info("Consumer started — listening on weather.readings.raw")

    conn  = get_pg_conn()
    batch = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value())
                batch.append(record)
            except json.JSONDecodeError as e:
                logger.error(f"Bad JSON: {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                flush_batch(batch, conn)
                consumer.commit(asynchronous=False)
                batch = []

    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        if batch:
            flush_batch(batch, conn)
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()