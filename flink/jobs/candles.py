import json
from datetime import datetime

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows


BROKER = "redpanda:29092"
SOURCE_TOPIC = "trades.raw"
SINK_TOPIC = "candles"


def parse_trade(message):
    trade = json.loads(message)

    return (
        trade["symbol"],
        float(trade["price"]),
        int(trade["size"]),
        int(
            datetime.fromisoformat(
                trade["event_timestamp"].replace("Z", "+00:00")
            ).timestamp()
            * 1000
        ),
    )


class TradeTimestampAssigner:
    def extract_timestamp(self, trade, record_timestamp):
        return trade[3]


class OhlcvAggregate(AggregateFunction):

    def create_accumulator(self):
        return (
            None,   # open price
            None,   # open timestamp
            None,   # high
            None,   # low
            None,   # close price
            None,   # close timestamp
            0,      # volume
        )

    def add(self, trade, accumulator):
        symbol, price, size, timestamp = trade

        (
            open_price,
            open_timestamp,
            high,
            low,
            close_price,
            close_timestamp,
            volume,
        ) = accumulator

        if open_timestamp is None or timestamp < open_timestamp:
            open_price = price
            open_timestamp = timestamp

        if high is None or price > high:
            high = price

        if low is None or price < low:
            low = price

        if close_timestamp is None or timestamp > close_timestamp:
            close_price = price
            close_timestamp = timestamp

        volume += size

        return (
            open_price,
            open_timestamp,
            high,
            low,
            close_price,
            close_timestamp,
            volume,
        )

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        if a[1] is None:
            return b

        if b[1] is None:
            return a

        open_price, open_timestamp = (
            (a[0], a[1])
            if a[1] <= b[1]
            else (b[0], b[1])
        )

        close_price, close_timestamp = (
            (a[4], a[5])
            if a[5] >= b[5]
            else (b[4], b[5])
        )

        return (
            open_price,
            open_timestamp,
            max(a[2], b[2]),
            min(a[3], b[3]),
            close_price,
            close_timestamp,
            a[6] + b[6],
        )


class AddWindowMetadata(ProcessWindowFunction):

    def __init__(self, window_size):
        self.window_size = window_size

    def process(self, key, context, aggregates):
        aggregate = next(iter(aggregates))

        (
            open_price,
            _,
            high,
            low,
            close_price,
            _,
            volume,
        ) = aggregate

        yield {
            "symbol": key,
            "window_size": self.window_size,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close_price,
            "volume": volume,
        }


class MovingAverageProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context):
        descriptor = ListStateDescriptor(
            "recent_closes",
            Types.DOUBLE(),
        )

        self.recent_closes = runtime_context.get_list_state(descriptor)

    def process_element(self, candle, ctx):
        closes = list(self.recent_closes.get())

        closes.append(candle["close"])

        # We never need more than 20 closes.
        closes = closes[-20:]

        self.recent_closes.update(closes)

        candle["sma_5"] = (
            sum(closes[-5:]) / 5
            if len(closes) >= 5
            else None
        )

        candle["sma_20"] = (
            sum(closes) / 20
            if len(closes) >= 20
            else None
        )

        yield candle


def to_json(candle):
    return json.dumps(candle)


def build_candles(trades, minutes):
    return (
        trades
        .key_by(lambda trade: trade[0])
        .window(
            TumblingEventTimeWindows.of(
                Time.minutes(minutes)
            )
        )
        .aggregate(
            OhlcvAggregate(),
            AddWindowMetadata(f"{minutes}m"),
        )
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKER)
        .set_topics(SOURCE_TOPIC)
        .set_group_id("flink-candles")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_trades = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Redpanda trades.raw",
    )

    trades = raw_trades.map(
        parse_trade,
        output_type=Types.TUPLE(
            [
                Types.STRING(),
                Types.DOUBLE(),
                Types.LONG(),
                Types.LONG(),
            ]
        ),
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TradeTimestampAssigner())
    )

    timestamped_trades = trades.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    candles_1m = build_candles(timestamped_trades, 1)
    candles_5m = build_candles(timestamped_trades, 5)
    candles_15m = build_candles(timestamped_trades, 15)

    windowed_candles = candles_1m.union(
        candles_5m,
        candles_15m,
    )

    candles_with_sma = (
        windowed_candles
        .key_by(
            lambda candle: (
                candle["symbol"],
                candle["window_size"],
            )
        )
        .process(MovingAverageProcessFunction())
    )

    candles = candles_with_sma.map(
        to_json,
        output_type=Types.STRING(),
    )

    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(SINK_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKER)
        .set_record_serializer(serializer)
        .build()
    )

    candles.sink_to(sink)

    env.execute("Streams of GAMMAN - OHLCV Candles")


if __name__ == "__main__":
    main()
