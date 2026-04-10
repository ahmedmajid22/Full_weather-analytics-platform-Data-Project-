import json
import logging
import time
from datetime import datetime

import requests
import psycopg2
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_READINGS  = "weather.readings.raw"
TOPIC_ALERTS    = "weather.alerts"
OPEN_METEO_URL  = "https://api.open-meteo.com/v1/forecast"
POLL_INTERVAL   = 300


def get_cities():
    conn = psycopg2.connect(
        host="postgres", database="airflow",
        user="airflow", password="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, country, continent, latitude, longitude
        FROM weather.cities ORDER BY id
    """)
    cities = [
        {"id": r[0], "name": r[1], "country": r[2],
         "continent": r[3], "latitude": float(r[4]), "longitude": float(r[5])}
        for r in cursor.fetchall()
    ]
    cursor.close()
    conn.close()
    return cities


def get_baselines():
    """30-day rolling mean and stddev per city for anomaly detection."""
    try:
        conn = psycopg2.connect(
            host="postgres", database="airflow",
            user="airflow", password="airflow"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT city_id,
                   AVG(temperature)    AS mean_temp,
                   STDDEV(temperature) AS stddev_temp
            FROM weather.weather_readings
            WHERE timestamp >= NOW() - INTERVAL '30 days'
              AND temperature IS NOT NULL
            GROUP BY city_id
            HAVING COUNT(*) >= 24
        """)
        baselines = {
            row[0]: {"mean": float(row[1]), "stddev": float(row[2])}
            for row in cursor.fetchall()
        }
        cursor.close()
        conn.close()
        logger.info(f"Loaded baselines for {len(baselines)} cities")
        return baselines
    except Exception as e:
        logger.warning(f"Could not load baselines: {e}")
        return {}


def create_topics():
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        topics = [
            NewTopic(TOPIC_READINGS, num_partitions=3, replication_factor=1,
                     config={"retention.ms": str(7 * 24 * 3600 * 1000)}),
            NewTopic(TOPIC_ALERTS, num_partitions=1, replication_factor=1),
        ]
        fs = admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic created: {topic}")
            except Exception as e:
                logger.info(f"Topic {topic}: {e}")
    except Exception as e:
        logger.error(f"Topic creation failed: {e}")


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered → {msg.topic()} partition [{msg.partition()}] offset {msg.offset()}")


def produce_cycle(cities, producer, baselines):
    success, errors = 0, 0

    for city in cities:
        try:
            resp = requests.get(OPEN_METEO_URL, params={
                "latitude":        city["latitude"],
                "longitude":       city["longitude"],
                "current_weather": True,
                "timezone":        "UTC",
            }, timeout=15)
            resp.raise_for_status()
            cw = resp.json().get("current_weather", {})

            reading = {
                "city_id":        city["id"],
                "city_name":      city["name"],
                "country":        city["country"],
                "continent":      city["continent"],
                "timestamp":      datetime.utcnow().isoformat() + "Z",
                "temperature":    cw.get("temperature"),
                "windspeed":      cw.get("windspeed"),
                "weather_code":   cw.get("weathercode"),
                "schema_version": "1.0",
            }

            producer.produce(
                topic=TOPIC_READINGS,
                key=str(city["id"]),
                value=json.dumps(reading),
                callback=delivery_callback,
            )
            success += 1

            baseline = baselines.get(city["id"])
            temp = reading.get("temperature")
            if baseline and temp is not None and baseline["stddev"] > 0:
                z_score = (temp - baseline["mean"]) / baseline["stddev"]
                if abs(z_score) > 2.0:
                    alert = {
                        **reading,
                        "alert_type":      "temperature_anomaly",
                        "z_score":         round(z_score, 3),
                        "baseline_mean":   round(baseline["mean"], 2),
                        "baseline_stddev": round(baseline["stddev"], 2),
                        "severity":        "extreme" if abs(z_score) > 3.0 else "significant",
                    }
                    producer.produce(
                        topic=TOPIC_ALERTS,
                        key=str(city["id"]),
                        value=json.dumps(alert),
                        callback=delivery_callback,
                    )
                    logger.warning(
                        f"ALERT — {city['name']}: {temp}°C "
                        f"(z={z_score:.2f}, baseline={baseline['mean']:.1f}°C)"
                    )

        except Exception as e:
            errors += 1
            logger.error(f"Error for {city['name']}: {e}")

    producer.flush()
    logger.info(f"Cycle done — {success} produced, {errors} errors")


def main():
    logger.info("Starting weather Kafka producer")
    create_topics()

    producer = Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "acks":               "all",
        "retries":            5,
        "enable.idempotence": True,
        "compression.type":   "snappy",
        "linger.ms":          100,
    })

    cities = get_cities()
    logger.info(f"Loaded {len(cities)} cities from database")

    cycle = 0
    while True:
        cycle += 1
        logger.info(f"--- Cycle {cycle} ---")
        baselines = get_baselines()
        produce_cycle(cities, producer, baselines)
        logger.info(f"Sleeping {POLL_INTERVAL}s")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()