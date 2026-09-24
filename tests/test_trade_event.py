import sys
import unittest
from pathlib import Path

PRODUCER_SRC = Path(__file__).parents[1] / "producer" / "src"
sys.path.insert(0, str(PRODUCER_SRC))

from trade_event import build_trade_event


class TestTradeEvent(unittest.TestCase):

    def test_build_trade_event(self):
        event = build_trade_event(
            symbol="nvda",
            trade_id=123456,
            price=338.21,
            size=5,
            event_timestamp="2026-09-21T14:30:00+00:00",
            exchange="V",
            conditions=["@"],
            tape="C",
            source="live",
        )

        self.assertEqual(
            event,
            {
                "event_id": "alpaca:iex:NVDA:123456",
                "symbol": "NVDA",
                "trade_id": 123456,
                "price": 338.21,
                "size": 5,
                "event_timestamp": "2026-09-21T14:30:00+00:00",
                "exchange": "V",
                "conditions": ["@"],
                "tape": "C",
                "feed": "iex",
                "source": "live",
            },
        )

    def test_event_id_is_independent_of_source(self):
        common_fields = {
            "symbol": "nvda",
            "trade_id": 123456,
            "price": 338.21,
            "size": 5,
            "event_timestamp": "2026-09-21T14:30:00+00:00",
            "exchange": "V",
            "conditions": ["@"],
            "tape": "C",
        }

        live_event = build_trade_event(
            **common_fields,
            source="live",
        )

        historical_event = build_trade_event(
            **common_fields,
            source="historical",
        )

        self.assertEqual(
            live_event["event_id"],
            historical_event["event_id"],
        )
        self.assertEqual(
            live_event["event_id"],
            "alpaca:iex:NVDA:123456",
        )

    def test_event_id_includes_feed(self):
        event = build_trade_event(
            symbol="nvda",
            trade_id=123456,
            price=338.21,
            size=5,
            event_timestamp="2026-09-21T14:30:00+00:00",
            exchange="V",
            conditions=["@"],
            tape="C",
            source="historical",
            feed="sip",
        )

        self.assertEqual(
            event["event_id"],
            "alpaca:sip:NVDA:123456",
        )

if __name__ == "__main__":
    unittest.main()
