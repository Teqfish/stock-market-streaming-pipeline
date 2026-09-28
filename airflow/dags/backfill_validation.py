from datetime import datetime


WINDOW_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
}


def floor_timestamp(
    timestamp: datetime,
    window_minutes: int,
) -> datetime:
    """Return the start of the candle containing a timestamp."""

    minute = (
        timestamp.minute // window_minutes
    ) * window_minutes

    return timestamp.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )


def build_expected_windows(
    events: list[dict],
    symbols: list[str],
) -> dict[str, dict[str, set[datetime]]]:
    """Build expected candle windows from canonical trade events."""

    expected_windows = {
        symbol: {
            window_size: set()
            for window_size in WINDOW_MINUTES
        }
        for symbol in symbols
    }

    for event in events:
        symbol = event["symbol"]

        if symbol not in expected_windows:
            raise ValueError(
                "Historical dataset contains unexpected symbol: "
                f"{symbol}"
            )

        timestamp = datetime.fromisoformat(
            event["event_timestamp"].replace(
                "Z",
                "+00:00",
            )
        )

        for window_size, minutes in WINDOW_MINUTES.items():
            expected_windows[symbol][window_size].add(
                floor_timestamp(timestamp, minutes)
            )

    return expected_windows


def compare_windows(
    expected: set[datetime],
    actual: set[datetime],
) -> tuple[set[datetime], set[datetime]]:
    """Return missing and unexpected candle windows."""

    missing = expected - actual
    unexpected = actual - expected

    return missing, unexpected
