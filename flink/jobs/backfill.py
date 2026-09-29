import argparse
import json
from datetime import datetime

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
    KafkaTopicPartition
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process a bounded historical trade backfill."
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
    )

    parser.add_argument(
        "--processing-start",
        required=True,
    )

    parser.add_argument(
        "--processing-end",
        required=True,
    )

    parser.add_argument(
        "--requested-start",
        required=True,
    )

    parser.add_argument(
        "--requested-end",
        required=True,
    )

    parser.add_argument(
        "--start-offsets",
        required=True,
        help='JSON mapping such as {"0": 100}',
    )

    parser.add_argument(
        "--end-offsets",
        required=True,
        help='JSON mapping such as {"0": 110}',
    )

    return parser.parse_args()


def parse_timestamp(value):
    timestamp = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError(
            "timestamp must include timezone information"
        )

    return int(timestamp.timestamp() * 1000)


def build_partition_offsets(offsets):
    return {
        KafkaTopicPartition(
            SOURCE_TOPIC,
            int(partition),
        ): int(offset)
        for partition, offset in offsets.items()
    }


def main():
    args = parse_args()

    symbols = {
        symbol.upper()
        for symbol in args.symbols
    }

    processing_start_timestamp = parse_timestamp(
        args.processing_start
    )
    processing_end_timestamp = parse_timestamp(
        args.processing_end
    )
    requested_start_timestamp = parse_timestamp(
        args.requested_start
    )
    requested_end_timestamp = parse_timestamp(
        args.requested_end
    )

    if not (
        processing_start_timestamp
        <= requested_start_timestamp
        < requested_end_timestamp
        <= processing_end_timestamp
    ):
        raise ValueError(
            "Requested range must fall within "
            "the processing range"
        )

    start_offsets = json.loads(args.start_offsets)
    end_offsets = json.loads(args.end_offsets)

    if set(start_offsets) != set(end_offsets):
        raise ValueError(
            "Start and end offset partitions do not match"
        )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKER)
        .set_partitions(
            set(
                build_partition_offsets(
                    start_offsets
                )
            )
        )
        .set_group_id("flink-backfill")
        .set_starting_offsets(
            KafkaOffsetsInitializer.offsets(
                build_partition_offsets(
                    start_offsets
                )
            )
        )
        .set_bounded(
            KafkaOffsetsInitializer.offsets(
                build_partition_offsets(
                    end_offsets
                )
            )
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    raw_trades = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Bounded Redpanda trades.raw",
    )

    parsed_trades = raw_trades.map(
        parse_trade,
        output_type=Types.TUPLE(
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
                Types.LONG(),
                Types.LONG(),
            ]
        ),
    )

    historical_trades = (
        parsed_trades
        .filter(
            lambda trade: (
                trade[1] == "historical"
                and trade[2] in symbols
                and processing_start_timestamp
                <= trade[5]
                < processing_end_timestamp
            )
        )
        .name("Historical Processing Range")
    )

    unique_trades = (
        historical_trades
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
        .name("Deduplicate Historical Trades")
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        )
        .with_timestamp_assigner(
            TradeTimestampAssigner()
        )
    )

    timestamped_trades = (
        unique_trades
        .assign_timestamps_and_watermarks(
            watermark_strategy
        )
    )

    candles_1m = build_candles(
        timestamped_trades,
        1,
    )

    candles_5m = build_candles(
        timestamped_trades,
        5,
    )

    candles_15m = build_candles(
        timestamped_trades,
        15,
    )

    windowed_candles = (
        candles_1m
        .union(
            candles_5m,
            candles_15m,
        )
    )

    candles_with_sma = (
        windowed_candles
        .key_by(
            lambda candle: (
                candle["symbol"],
                candle["window_size"],
            )
        )
        .process(
            MovingAverageProcessFunction()
        )
        .name("Historical Moving Averages")
    )

    requested_candles = (
        candles_with_sma
        .filter(
            lambda candle: (
                requested_start_timestamp
                <= candle["window_start"]
                < requested_end_timestamp
            )
        )
        .name("Requested Session Candles")
    )

    candles = (
        requested_candles
        .map(
            to_json,
            output_type=Types.STRING(),
        )
    )

    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(SINK_TOPIC)
        .set_value_serialization_schema(
            SimpleStringSchema()
        )
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKER)
        .set_record_serializer(serializer)
        .build()
    )

    candles.sink_to(sink)

    env.execute(
        "Streams of GAMMAN - Historical Backfill"
    )


if __name__ == "__main__":
    main()
