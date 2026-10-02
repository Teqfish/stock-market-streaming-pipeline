import json
from datetime import datetime

from pyflink.common import Time, Types
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import (
    ListStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)
from pyflink.datastream.window import TumblingEventTimeWindows


def parse_trade(message):
    """Parse a canonical raw trade event into the Flink trade tuple."""
    trade = json.loads(message)

    return (
        trade["event_id"],
        trade["source"],
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
    """Extract event time from a parsed canonical trade tuple."""

    def extract_timestamp(self, trade, record_timestamp):
        return trade[3]


class DeduplicateTrade(KeyedProcessFunction):
    """Drop canonical trade events whose event_id has already been seen."""

    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig
            .new_builder(Time.hours(24))
            .set_update_type(
                StateTtlConfig.UpdateType.OnCreateAndWrite
            )
            .set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            )
            .build()
        )

        descriptor = ValueStateDescriptor(
            "seen",
            Types.BOOLEAN(),
        )

        descriptor.enable_time_to_live(ttl_config)

        self.seen = runtime_context.get_state(descriptor)

        self.duplicates_dropped = (
            runtime_context
            .get_metrics_group()
            .counter("duplicates_dropped")
        )

    def process_element(self, trade, ctx):
        if self.seen.value():
            self.duplicates_dropped.inc()
            return

        self.seen.update(True)

        yield (
            trade[2],
            trade[3],
            trade[4],
            trade[5],
        )


class OhlcvAggregate(AggregateFunction):
    """Incrementally calculate OHLCV values for a candle window."""

    def create_accumulator(self):
        return (
            None,  # open price
            None,  # open timestamp
            None,  # high
            None,  # low
            None,  # close price
            None,  # close timestamp
            0,     # volume
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
    """Attach symbol, window size and window boundaries to OHLCV."""

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
    """Calculate SMA-5 and SMA-20 independently per symbol/window size."""

    def open(self, runtime_context):
        descriptor = ListStateDescriptor(
            "recent_closes",
            Types.DOUBLE(),
        )

        self.recent_closes = runtime_context.get_list_state(descriptor)

    def process_element(self, candle, ctx):
        closes = list(self.recent_closes.get())

        closes.append(candle["close"])

        # SMA-20 is the longest moving average we calculate.
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
    """Serialize a candle for publication to Redpanda."""
    return json.dumps(candle)


def build_candles(trades, minutes):
    """Build event-time OHLCV candles of the requested duration."""
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
