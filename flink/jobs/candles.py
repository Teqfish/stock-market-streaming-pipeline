from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    table_env.execute_sql("""
        CREATE TABLE trades (
            symbol STRING,
            price DOUBLE,
            size BIGINT,
            event_timestamp STRING,

            event_time AS TO_TIMESTAMP_LTZ(
                UNIX_TIMESTAMP(event_timestamp) * 1000,
                3
            ),

            WATERMARK FOR event_time
                AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'trades.raw',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-candles',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'false'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE candles (
            symbol STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            low DOUBLE,
            high DOUBLE,
            volume BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'candles',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'format' = 'json'
        )
    """)

    result = table_env.execute_sql("""
        INSERT INTO candles
        SELECT
            symbol,
            window_start,
            window_end,
            MIN(price) AS low,
            MAX(price) AS high,
            SUM(size) AS volume
        FROM TABLE(
            TUMBLE(
                TABLE trades,
                DESCRIPTOR(event_time),
                INTERVAL '1' MINUTE
            )
        )
        GROUP BY
            symbol,
            window_start,
            window_end
    """)

    result.wait()


if __name__ == "__main__":
    main()
