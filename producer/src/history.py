from datetime import datetime

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest

from producer.src.trade_event import build_trade_event


def get_historical_events(
    *,
    api_key: str,
    secret_key: str,
    symbols: list[str],
    start: datetime,
    end: datetime,
) -> list[dict]:
    client = StockHistoricalDataClient(
        api_key,
        secret_key,
    )

    request = StockTradesRequest(
        symbol_or_symbols=symbols,
        start=start,
        end=end,
        feed=DataFeed.IEX,
    )

    trades = client.get_stock_trades(request)

    events = []

    for symbol in symbols:
        for trade in trades[symbol]:
            events.append(
                build_trade_event(
                    symbol=trade.symbol,
                    trade_id=trade.id,
                    price=float(trade.price),
                    size=int(trade.size),
                    event_timestamp=trade.timestamp.isoformat(),
                    exchange=trade.exchange,
                    conditions=trade.conditions,
                    tape=trade.tape,
                    source="historical",
                )
            )

    return events
