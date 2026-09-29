import json
import os
import urllib.error
import urllib.request
import exchange_calendars as xcals
import time
import psycopg2
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, task

from history import get_historical_events
from redpanda import get_high_watermarks, publish_events
from backfill_validation import (
    WINDOW_MINUTES,
    build_expected_windows,
    compare_windows,
)


MARKET_CALENDAR = "XNYS"
BACKFILL_DATA_DIR = Path("/opt/airflow/data/backfills")
REDPANDA_BROKER = os.environ.get(
    "REDPANDA_BROKER",
    "redpanda:29092",
)
RAW_TRADES_TOPIC = "trades.raw"
FLINK_SUBMITTER_URL = os.environ.get(
    "FLINK_SUBMITTER_URL",
    "http://flink-submitter:8090",
)


def parse_timestamp(value):
    timestamp = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError(
            "timestamp must include timezone information"
        )

    return timestamp.astimezone(timezone.utc)


@dag(
    dag_id="market_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["recovery", "market-data"],
    params={
        "symbols": ["NVDA"],
        "trading_date": "2026-09-21",
    },
)
def market_backfill():

    @task
    def validate_request(**context):
        params = context["params"]

        symbols = params["symbols"]
        trading_date_raw = params["trading_date"]

        if not isinstance(symbols, list) or not symbols:
            raise AirflowFailException(
                "symbols must be a non-empty list"
            )

        cleaned_symbols = []

        for symbol in symbols:
            if not isinstance(symbol, str) or not symbol.strip():
                raise AirflowFailException(
                    "every symbol must be a non-empty string"
                )

            cleaned_symbols.append(
                symbol.strip().upper()
            )

        if not isinstance(trading_date_raw, str):
            raise AirflowFailException(
                "trading_date must be YYYY-MM-DD"
            )

        try:
            trading_date = datetime.strptime(
                trading_date_raw,
                "%Y-%m-%d",
            ).date()
        except ValueError as exc:
            raise AirflowFailException(
                "trading_date must be YYYY-MM-DD"
            ) from exc

        if trading_date > datetime.now(timezone.utc).date():
            raise AirflowFailException(
                "trading_date cannot be in the future"
            )

        return {
            "symbols": cleaned_symbols,
            "trading_date": trading_date.isoformat(),
        }

    @task
    def resolve_market_session(request):
        calendar = xcals.get_calendar(
            MARKET_CALENDAR
        )

        trading_date = request["trading_date"]

        if not calendar.is_session(trading_date):
            raise AirflowFailException(
                f"{trading_date} is not a "
                f"{MARKET_CALENDAR} trading session"
            )

        session = calendar.schedule.loc[
            trading_date
        ]

        start = session["open"].to_pydatetime()
        end = session["close"].to_pydatetime()

        now = datetime.now(timezone.utc)

        if end > now:
            raise AirflowFailException(
                "trading session has not finished yet"
            )

        return {
            "symbols": request["symbols"],
            "trading_date": trading_date,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    @task
    def resolve_processing_range(session: dict) -> dict:
        """Extend the requested session with a preceding XNYS session for SMA warm-up."""

        calendar = xcals.get_calendar("XNYS")

        requested_start = parse_timestamp(
            session["start"]
        )

        requested_label = calendar.date_to_session(
            requested_start.date(),
            direction="none",
        )

        warmup_label = calendar.previous_session(
            requested_label
        )

        warmup_open = calendar.session_open(
            warmup_label
        ).to_pydatetime()

        return {
            "symbols": session["symbols"],
            "start": warmup_open.isoformat(),
            "end": session["end"],
            "requested_start": session["start"],
            "requested_end": session["end"],
        }

    @task
    def fetch_historical_trades(
        processing_range: dict,
        **context,
    ) -> dict:
        run_id = context["run_id"]

        start = parse_timestamp(
            processing_range["start"]
        )

        end = parse_timestamp(
            processing_range["end"]
        )

        events = get_historical_events(
            api_key=os.environ["ALPACA_API_KEY"],
            secret_key=os.environ["ALPACA_SECRET_KEY"],
            symbols=processing_range["symbols"],
            start=start,
            end=end,
        )

        if not events:
            raise AirflowFailException(
                "Alpaca returned no historical trades "
                "for the requested interval"
            )

        BACKFILL_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        safe_run_id = (
            run_id
            .replace(":", "_")
            .replace("/", "_")
        )

        path = BACKFILL_DATA_DIR / f"{safe_run_id}.json"

        with path.open("w") as file:
            json.dump(events, file)

        return {
            "path": str(path),
            "event_count": len(events),
        }

    @task
    def validate_historical_trades(
        processing_range,
        dataset,
    ):
        path = Path(dataset["path"])

        if not path.exists():
            raise AirflowFailException(
                f"historical dataset does not exist: {path}"
            )

        with path.open() as file:
            events = json.load(file)

        if len(events) != dataset["event_count"]:
            raise AirflowFailException(
                "historical dataset event count changed "
                "between fetch and validation"
            )

        requested_symbols = set(
            processing_range["symbols"]
        )
        start = parse_timestamp(
            processing_range["start"]
        )
        end = parse_timestamp(
            processing_range["end"]
        )

        seen_event_ids = set()

        for index, event in enumerate(events):
            required_fields = {
                "event_id",
                "symbol",
                "trade_id",
                "price",
                "size",
                "event_timestamp",
                "feed",
                "source",
            }

            missing_fields = required_fields - event.keys()

            if missing_fields:
                raise AirflowFailException(
                    f"event {index} is missing required fields: "
                    f"{sorted(missing_fields)}"
                )

            if event["source"] != "historical":
                raise AirflowFailException(
                    f"event {index} has invalid source "
                    f"{event['source']!r}"
                )

            if event["feed"] != "iex":
                raise AirflowFailException(
                    f"event {index} has unexpected feed "
                    f"{event['feed']!r}"
                )

            if event["symbol"] not in requested_symbols:
                raise AirflowFailException(
                    f"event {index} contains unrequested symbol "
                    f"{event['symbol']!r}"
                )

            try:
                timestamp = parse_timestamp(
                    event["event_timestamp"]
                )
            except (AttributeError, ValueError) as exc:
                raise AirflowFailException(
                    f"event {index} has invalid event_timestamp"
                ) from exc

            if not start <= timestamp < end:
                raise AirflowFailException(
                    f"event {index} falls outside the "
                    "processing interval"
                )

            if (
                not isinstance(event["price"], (int, float))
                or isinstance(event["price"], bool)
                or event["price"] <= 0
            ):
                raise AirflowFailException(
                    f"event {index} has invalid price"
                )

            if (
                not isinstance(event["size"], int)
                or isinstance(event["size"], bool)
                or event["size"] <= 0
            ):
                raise AirflowFailException(
                    f"event {index} has invalid size"
                )

            event_id = event["event_id"]

            if not isinstance(event_id, str) or not event_id:
                raise AirflowFailException(
                    f"event {index} has invalid event_id"
                )

            if event_id in seen_event_ids:
                raise AirflowFailException(
                    f"duplicate event_id returned by Alpaca: "
                    f"{event_id}"
                )

            seen_event_ids.add(event_id)

        return {
            "path": str(path),
            "event_count": len(events),
            "symbols": sorted(requested_symbols),
            "start": processing_range["start"],
            "end": processing_range["end"],
            "requested_start": processing_range[
                "requested_start"
            ],
            "requested_end": processing_range[
                "requested_end"
            ],
        }

    @task
    def capture_start_offsets():
        offsets = {
            str(partition): offset
            for partition, offset in get_high_watermarks(
                broker=REDPANDA_BROKER,
                topic=RAW_TRADES_TOPIC,
            ).items()
        }

        if not offsets:
            raise AirflowFailException(
                f"No partitions found for {RAW_TRADES_TOPIC}"
            )

        return offsets

    @task
    def publish_historical_trades(validated_dataset):
        path = Path(validated_dataset["path"])

        if not path.exists():
            raise AirflowFailException(
                f"Validated historical dataset does not exist: "
                f"{path}"
            )

        with path.open() as file:
            events = json.load(file)

        expected_count = validated_dataset["event_count"]

        if len(events) != expected_count:
            raise AirflowFailException(
                "Historical dataset event count changed "
                "after validation"
            )

        published_count = publish_events(
            events=events,
            broker=REDPANDA_BROKER,
            topic=RAW_TRADES_TOPIC,
        )

        if published_count != expected_count:
            raise AirflowFailException(
                f"Expected to publish {expected_count} events, "
                f"but published {published_count}"
            )

        return {
            "event_count": published_count,
        }

    @task
    def capture_end_offsets(
        start_offsets,
        publication,
    ):
        end_offsets = {
            str(partition): offset
            for partition, offset in get_high_watermarks(
                broker=REDPANDA_BROKER,
                topic=RAW_TRADES_TOPIC,
            ).items()
        }

        start_partitions = set(start_offsets)
        end_partitions = set(end_offsets)

        if start_partitions != end_partitions:
            raise AirflowFailException(
                "Topic partitions changed during publication"
            )

        records_added = sum(
            end_offsets[partition]
            - start_offsets[partition]
            for partition in start_offsets
        )

        if records_added < publication["event_count"]:
            raise AirflowFailException(
                f"Expected at least "
                f"{publication['event_count']} new records, "
                f"but offset range contains only "
                f"{records_added}"
            )

        return {
            "start_offsets": start_offsets,
            "end_offsets": end_offsets,
            "published_count": publication["event_count"],
            "records_in_range": records_added,
        }

    @task
    def run_bounded_flink_backfill(
        session,
        processing_range,
        offset_range,
    ):
        payload = {
            "symbols": session["symbols"],
            "processing_start": processing_range["start"],
            "processing_end": processing_range["end"],
            "requested_start": session["start"],
            "requested_end": session["end"],
            "start_offsets": offset_range["start_offsets"],
            "end_offsets": offset_range["end_offsets"],
        }

        request = urllib.request.Request(
            f"{FLINK_SUBMITTER_URL}/backfills",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=120,
            ) as response:
                result = json.loads(
                    response.read().decode("utf-8")
                )
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode(
                "utf-8",
                errors="replace",
            )

            raise AirflowFailException(
                "Bounded Flink backfill failed: "
                f"HTTP {exc.code}: {error_body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise AirflowFailException(
                "Could not reach Flink submitter: "
                f"{exc.reason}"
            ) from exc

        if result.get("status") != "finished":
            raise AirflowFailException(
                "Flink submitter returned unexpected result: "
                f"{result}"
            )

        return {
            "status": result["status"],
            "published_count": offset_range[
                "published_count"
            ],
            "records_in_range": offset_range[
                "records_in_range"
            ],
        }

    @task
    def validate_postgres_backfill(
        session: dict,
        validated_dataset: dict,
    ) -> None:
        """Wait for reconstructed candles to reach PostgreSQL and validate them."""

        symbols = session["symbols"]
        session_open = datetime.fromisoformat(
            session["start"]
        )
        session_close = datetime.fromisoformat(
            session["end"]
        )

        dataset_path = Path(
            validated_dataset["path"]
        )

        if not dataset_path.exists():
            raise AirflowFailException(
                f"Validated historical dataset does not exist: "
                f"{dataset_path}"
            )

        with dataset_path.open() as file:
            events = json.load(file)

        requested_events = []

        for event in events:
            event_timestamp = parse_timestamp(
                event["event_timestamp"]
            )

            if (
                session_open
                <= event_timestamp
                < session_close
            ):
                requested_events.append(event)

        if not requested_events:
            raise AirflowFailException(
                "Validated historical dataset contains no "
                "events in the requested session"
            )

        try:
            expected_windows = build_expected_windows(
                requested_events,
                symbols,
            )
        except ValueError as exc:
            raise AirflowFailException(
                str(exc)
            ) from exc

        expected_window_sizes = set(
            WINDOW_MINUTES
        )

        timeout_seconds = 60
        poll_interval_seconds = 2
        deadline = (
            time.monotonic()
            + timeout_seconds
        )

        connection_kwargs = {
            "host": os.environ[
                "PIPELINE_POSTGRES_HOST"
            ],
            "port": os.environ.get(
                "PIPELINE_POSTGRES_PORT",
                "5432",
            ),
            "dbname": os.environ[
                "PIPELINE_POSTGRES_DB"
            ],
            "user": os.environ[
                "PIPELINE_POSTGRES_USER"
            ],
            "password": os.environ[
                "PIPELINE_POSTGRES_PASSWORD"
            ],
        }

        query = """
            SELECT
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
            FROM candles
            WHERE symbol = %s
            AND window_start >= %s
            AND window_start < %s
            ORDER BY window_size, window_start;
        """

        while True:
            problems = []

            with psycopg2.connect(
                **connection_kwargs
            ) as conn:
                with conn.cursor() as cursor:
                    for symbol in symbols:
                        cursor.execute(
                            query,
                            (
                                symbol,
                                session_open,
                                session_close,
                            ),
                        )

                        rows = cursor.fetchall()

                        actual_windows = {
                            window_size: set()
                            for window_size
                            in expected_window_sizes
                        }

                        for (
                            window_size,
                            window_start,
                            window_end,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            volume,
                            sma_5,
                            sma_20,
                        ) in rows:

                            if (
                                window_size
                                not in expected_window_sizes
                            ):
                                continue

                            actual_windows[
                                window_size
                            ].add(
                                window_start
                            )

                            if any(
                                value is None
                                for value in (
                                    open_price,
                                    high_price,
                                    low_price,
                                    close_price,
                                    volume,
                                )
                            ):
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle contains NULL OHLCV"
                                )
                                continue

                            if (
                                sma_5 is None
                                or sma_20 is None
                            ):
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle contains NULL SMA"
                                )

                            if volume <= 0:
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle has invalid volume"
                                )

                            if not (
                                low_price
                                <= open_price
                                <= high_price
                                and low_price
                                <= close_price
                                <= high_price
                            ):
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle violates OHLC bounds"
                                )

                            if (
                                window_start
                                < session_open
                            ):
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle starts before session"
                                )

                            if (
                                window_end
                                > session_close
                            ):
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    "candle ends after session"
                                )

                        for (
                            window_size
                        ) in expected_window_sizes:
                            expected = (
                                expected_windows[
                                    symbol
                                ][window_size]
                            )

                            actual = (
                                actual_windows[
                                    window_size
                                ]
                            )

                            (
                                missing,
                                unexpected,
                            ) = compare_windows(
                                expected,
                                actual,
                            )

                            if missing:
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    f"missing {len(missing)} of "
                                    f"{len(expected)} "
                                    "expected candles"
                                )

                            if unexpected:
                                problems.append(
                                    f"{symbol} {window_size}: "
                                    f"contains "
                                    f"{len(unexpected)} "
                                    "unexpected candles"
                                )

            if not problems:
                print(
                    "PostgreSQL backfill validation "
                    "passed for "
                    f"{', '.join(symbols)}."
                )
                return

            if (
                time.monotonic()
                >= deadline
            ):
                raise AirflowFailException(
                    "PostgreSQL backfill validation "
                    "failed after "
                    f"{timeout_seconds}s: "
                    f"{'; '.join(problems)}"
                )

            print(
                "PostgreSQL backfill not valid yet; "
                f"retrying in "
                f"{poll_interval_seconds}s: "
                f"{'; '.join(problems)}"
                )

            time.sleep(
                poll_interval_seconds
            )

    request = validate_request()

    session = resolve_market_session(request)

    processing_range = resolve_processing_range(
        session
    )

    dataset = fetch_historical_trades(
        processing_range
    )

    validated_dataset = validate_historical_trades(
        processing_range,
        dataset,
    )

    start_offsets = capture_start_offsets()

    publication = publish_historical_trades(
        validated_dataset,
    )

    end_offsets = capture_end_offsets(
        start_offsets,
        publication,
    )

    flink_backfill = run_bounded_flink_backfill(
        session,
        processing_range,
        end_offsets,
    )

    postgres_validation = validate_postgres_backfill(
        session=session,
        validated_dataset=validated_dataset,
    )

    validated_dataset >> start_offsets
    start_offsets >> publication
    publication >> end_offsets
    end_offsets >> flink_backfill
    flink_backfill >> postgres_validation


market_backfill()
