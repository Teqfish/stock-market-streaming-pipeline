import argparse
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

from history import get_historical_events


API_KEY = os.environ["ALPACA_API_KEY"]
SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]

REDPANDA_BROKER = os.getenv(
    "REDPANDA_BROKER",
    "redpanda:9092",
)

TOPIC = os.getenv(
    "REDPANDA_TOPIC",
    "trades.raw",
)


def parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError(
            f"Timestamp must include a timezone: {value}"
        )

    return timestamp.astimezone(timezone.utc)


def publish_events(events: list[dict]) -> None:
    producer = Producer(
        {
            "bootstrap.servers": REDPANDA_BROKER,
        }
    )

    for event in events:
        producer.produce(
            topic=TOPIC,
            key=event["symbol"],
            value=json.dumps(event),
        )

        producer.poll(0)

    producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill historical Alpaca IEX trades."
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="One or more US equity ticker symbols.",
    )

    parser.add_argument(
        "--start",
        required=True,
        type=parse_timestamp,
        help="Backfill start timestamp in ISO 8601 format.",
    )

    parser.add_argument(
        "--end",
        required=True,
        type=parse_timestamp,
        help="Backfill end timestamp in ISO 8601 format.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    events = get_historical_events(
        api_key=API_KEY,
        secret_key=SECRET_KEY,
        symbols=args.symbols,
        start=args.start,
        end=args.end,
    )

    print(
        f"Publishing {len(events)} historical trades "
        f"to {TOPIC}"
    )

    publish_events(events)

    print(f"Published {len(events)} historical trades")
