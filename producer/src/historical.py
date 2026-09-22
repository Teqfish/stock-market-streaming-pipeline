import argparse
import os, json
from datetime import datetime, timezone

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest

from trade_event import build_trade_event
from confluent_kafka import Producer


API_KEY = os.environ["ALPACA_API_KEY"]
SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
TOPIC = os.getenv("REDPANDA_TOPIC", "trades.raw")


def get_historical_trades(
    symbols: list[str],
    start: datetime,
    end: datetime,
):
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

    request = StockTradesRequest(
        symbol_or_symbols=symbols,
        start=start,
        end=end,
        feed=DataFeed.IEX,
    )

    return client.get_stock_trades(request)


def parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if timestamp.tzinfo is None:
        raise ValueError(
            f"Timestamp must include a timezone: {value}"
        )

    return timestamp.astimezone(timezone.utc)


def historical_trade_to_event(trade) -> dict:
    return build_trade_event(
        symbol=trade.symbol,
        trade_id=trade.id,
        price=float(trade.price),
        size=int(trade.size),
        event_timestamp=trade.timestamp.isoformat(),
        exchange=trade.exchange,
        conditions=trade.conditions,
        tape=trade.tape,
        source="historical",
    )


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
        description="Retrieve historical Alpaca IEX trades."
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

    trades = get_historical_trades(
        symbols=args.symbols,
        start=args.start,
        end=args.end,
    )

    events = []

    for symbol in args.symbols:
        symbol_trades = trades[symbol]

        print(f"{symbol}: {len(symbol_trades)} trades")

        for trade in symbol_trades:
            events.append(historical_trade_to_event(trade))

    print(f"Publishing {len(events)} trades to {TOPIC}")

    publish_events(events)

    print(f"Published {len(events)} trades")
