from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from processing import (
    DeduplicateTrade,
    MovingAverageProcessFunction,
    TradeTimestampAssigner,
    build_candles,
    parse_trade,
    to_json,
)


BROKER = "redpanda:29092"
SOURCE_TOPIC = "trades.raw"
SINK_TOPIC = "candles"


def is_live_trade(trade):
    """Return True for records handled by the continuous pipeline."""
    return trade[1] in ("", "live")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000)
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

    parsed_trades = raw_trades.map(
        parse_trade,
        output_type=Types.TUPLE(
            [
                Types.STRING(),  # event_id
                Types.STRING(),  # source
                Types.STRING(),  # symbol
                Types.DOUBLE(),  # price
                Types.LONG(),    # size
                Types.LONG(),    # timestamp
            ]
        ),
    )

    live_trades = parsed_trades.filter(
        is_live_trade
    ).name("Live Trades Only")

    # Legacy records pre-date canonical event_id/source fields.
    # Preserve compatibility until old development data is retired.
    legacy_trades = (
        live_trades
        .filter(lambda trade: trade[0] == "")
        .map(
            lambda trade: (
                trade[2],
                trade[3],
                trade[4],
                trade[5],
            ),
            output_type=Types.TUPLE(
                [
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.LONG(),
                    Types.LONG(),
                ]
            ),
        )
        .name("Legacy Trades")
    )

    canonical_trades = (
        live_trades
        .filter(lambda trade: trade[0] != "")
        .key_by(lambda trade: trade[0])
        .process(
            DeduplicateTrade(),
            output_type=Types.TUPLE(
                [
                    Types.STRING(),
                    Types.DOUBLE(),
                    Types.LONG(),
                    Types.LONG(),
                ]
            ),
        )
        .name("Deduplicate Trades")
    )

    unique_trades = legacy_trades.union(canonical_trades)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TradeTimestampAssigner())
    )

    timestamped_trades = (
        unique_trades
        .assign_timestamps_and_watermarks(watermark_strategy)
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
