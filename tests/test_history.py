import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from history import get_historical_events


class TestHistoricalEvents(unittest.TestCase):
    @patch("history.StockHistoricalDataClient")
    def test_converts_alpaca_trade_to_canonical_event(
        self,
        mock_client_class,
    ):
        trade = MagicMock()
        trade.symbol = "NVDA"
        trade.id = 7453
        trade.price = 223.61
        trade.size = 5
        trade.timestamp = datetime(
            2026,
            9,
            21,
            14,
            31,
            tzinfo=timezone.utc,
        )
        trade.exchange = "V"
        trade.conditions = ["@", "I"]
        trade.tape = "C"

        client = mock_client_class.return_value
        client.get_stock_trades.return_value = {
            "NVDA": [trade]
        }

        events = get_historical_events(
            api_key="test-key",
            secret_key="test-secret",
            symbols=["NVDA"],
            start=datetime(
                2026,
                9,
                21,
                14,
                30,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                21,
                14,
                32,
                tzinfo=timezone.utc,
            ),
        )

        self.assertEqual(len(events), 1)

        event = events[0]

        self.assertEqual(
            event["event_id"],
            "alpaca:iex:NVDA:7453",
        )
        self.assertEqual(event["trade_id"], 7453)
        self.assertEqual(event["symbol"], "NVDA")
        self.assertEqual(event["source"], "historical")
        self.assertEqual(event["price"], 223.61)
        self.assertEqual(event["size"], 5)
        self.assertEqual(
            event["event_timestamp"],
            "2026-09-21T14:31:00+00:00",
        )
        self.assertEqual(event["exchange"], "V")
        self.assertEqual(event["conditions"], ["@", "I"])
        self.assertEqual(event["tape"], "C")
        self.assertEqual(event["feed"], "iex")


if __name__ == "__main__":
    unittest.main()
