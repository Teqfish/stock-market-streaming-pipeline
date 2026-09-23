import argparse
import os
from datetime import datetime, timezone

from history import get_historical_events
from redpanda import publish_events


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

    published_count = publish_events(
        events=events,
        broker=REDPANDA_BROKER,
        topic=TOPIC,
    )

    print(f"Published {published_count} historical trades")
