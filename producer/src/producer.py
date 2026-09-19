import json
import os

from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from confluent_kafka import Producer


ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
TICKERS = os.getenv("TICKERS", "GOOGL,AAPL,META,MSFT,AMZN,NVDA").split(",")
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:29092")
TOPIC = "trades.raw"


producer = Producer(
    {
        "bootstrap.servers": REDPANDA_BROKER,
    }
)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Published {msg.key().decode()} "
            f"to {msg.topic()} [{msg.partition()}] "
            f"offset {msg.offset()}"
        )


async def handle_trade(trade):
    event = {
        "symbol": trade.symbol,
        "price": float(trade.price),
        "size": int(trade.size),
        "event_timestamp": trade.timestamp.isoformat(),
    }

    producer.produce(
        topic=TOPIC,
        key=trade.symbol,
        value=json.dumps(event),
        callback=delivery_report,
    )

    producer.poll(0)


def main():
    stream = StockDataStream(
        ALPACA_API_KEY,
        ALPACA_SECRET_KEY,
        feed=DataFeed.IEX,
    )

    stream.subscribe_trades(handle_trade, *TICKERS)

    print(f"Streaming trades for: {', '.join(TICKERS)}")
    print(f"Publishing to: {TOPIC}")

    stream.run()


if __name__ == "__main__":
    main()
