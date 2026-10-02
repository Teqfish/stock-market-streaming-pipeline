from datetime import datetime


def build_trade_event(
    *,
    symbol: str,
    trade_id: int,
    price: float,
    size: int,
    event_timestamp: str,
    exchange: str | None,
    conditions: list[str] | None,
    tape: str | None,
    source: str,
    feed: str = "iex",
) -> dict:
    """Build the canonical raw trade event published to trades.raw."""

    symbol = symbol.upper()

    timestamp = datetime.fromisoformat(
        event_timestamp.replace("Z", "+00:00")
    )

    trading_date = timestamp.date().isoformat()

    event_id = (
        f"alpaca:{feed}:{symbol}:"
        f"{trading_date}:{trade_id}"
    )

    return {
        "event_id": event_id,
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "size": size,
        "event_timestamp": event_timestamp,
        "exchange": exchange,
        "conditions": conditions or [],
        "tape": tape,
        "feed": feed,
        "source": source,
    }
