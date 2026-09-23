import json, os
import exchange_calendars as xcals
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, task

from history import get_historical_events
from redpanda import get_high_watermarks, publish_events


MARKET_CALENDAR = "XNYS"
MAX_BACKFILL_DAYS = 5
BACKFILL_DATA_DIR = Path("/opt/airflow/data/backfills")
REDPANDA_BROKER = os.environ.get(
    "REDPANDA_BROKER",
    "redpanda:29092",
)
RAW_TRADES_TOPIC = "trades.raw"


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

        if (now - start).days > MAX_BACKFILL_DAYS:
            raise AirflowFailException(
                f"trading session cannot be more than "
                f"{MAX_BACKFILL_DAYS} days ago"
            )

        return {
            "symbols": request["symbols"],
            "trading_date": trading_date,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    @task
    def fetch_historical_trades(request, **context):
        run_id = context["run_id"]

        api_key = os.environ.get("ALPACA_API_KEY")
        secret_key = os.environ.get("ALPACA_SECRET_KEY")

        if not api_key or not secret_key:
            raise AirflowFailException(
                "Alpaca API credentials are not configured"
            )

        start = parse_timestamp(request["start"])
        end = parse_timestamp(request["end"])

        events = get_historical_events(
            api_key=api_key,
            secret_key=secret_key,
            symbols=request["symbols"],
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
    def validate_historical_trades(request, dataset):
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

        requested_symbols = set(request["symbols"])
        start = parse_timestamp(request["start"])
        end = parse_timestamp(request["end"])

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
                    "requested interval"
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
            "start": request["start"],
            "end": request["end"],
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

    request = validate_request()

    session = resolve_market_session(request)

    dataset = fetch_historical_trades(session)

    validated_dataset = validate_historical_trades(
        session,
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

    validated_dataset >> start_offsets
    start_offsets >> publication
    publication >> end_offsets


market_backfill()
