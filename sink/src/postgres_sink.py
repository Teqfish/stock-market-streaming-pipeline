import json
import os
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer


BROKER = os.getenv("REDPANDA_BROKER", "redpanda:29092")
TOPIC = "candles"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


UPSERT_CANDLE = """
    INSERT INTO candles (
        symbol,
        window_size,
        window_start,
        window_end,
        open,
        high,
        low,
        close,
        volume,
        sma_5,
        sma_20
    )
    VALUES (
        %(symbol)s,
        %(window_size)s,
        %(window_start)s,
        %(window_end)s,
        %(open)s,
        %(high)s,
        %(low)s,
        %(close)s,
        %(volume)s,
        %(sma_5)s,
        %(sma_20)s
    )
    ON CONFLICT (symbol, window_size, window_start)
    DO UPDATE SET
        window_end = EXCLUDED.window_end,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        sma_5 = EXCLUDED.sma_5,
        sma_20 = EXCLUDED.sma_20;
"""


def connect_to_postgres():
    while True:
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )

            print("Connected to PostgreSQL.")
            return connection

        except psycopg2.OperationalError as error:
            print(f"PostgreSQL unavailable: {error}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


def epoch_ms_to_datetime(timestamp_ms):
    return datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc,
    )


def prepare_candle(candle):
    return {
        **candle,
        "window_start": epoch_ms_to_datetime(candle["window_start"]),
        "window_end": epoch_ms_to_datetime(candle["window_end"]),
        "sma_5": candle.get("sma_5"),
        "sma_20": candle.get("sma_20"),
    }


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "postgres-candles",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([TOPIC])

    connection = connect_to_postgres()

    print(f"Consuming candles from: {TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            try:
                candle = json.loads(message.value().decode("utf-8"))
                candle = prepare_candle(candle)

                with connection.cursor() as cursor:
                    cursor.execute(UPSERT_CANDLE, candle)

                connection.commit()

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print(
                    f"Stored {candle['symbol']} "
                    f"{candle['window_size']} "
                    f"{candle['window_start']}"
                )

            except Exception as error:
                connection.rollback()
                print(f"Failed to store candle: {error}")

    finally:
        connection.close()
        consumer.close()


if __name__ == "__main__":
    main()
