import unittest

from trade_event import build_trade_event


class TestTradeEvent(unittest.TestCase):
    def test_build_trade_event(self):
        event = build_trade_event(
            symbol="aapl",
            trade_id=12345,
            price=189.25,
            size=100,
            event_timestamp="2026-09-21T14:30:00.123456+00:00",
            exchange="V",
            conditions=["@"],
            tape="C",
            source="live",
            feed="iex",
        )

        self.assertEqual(
            event["event_id"],
            "alpaca:iex:AAPL:2026-09-21:12345",
        )
        self.assertEqual(
            event["symbol"],
            "AAPL",
        )
        self.assertEqual(
            event["trade_id"],
            12345,
        )
        self.assertEqual(
            event["source"],
            "live",
        )
        self.assertEqual(
            event["feed"],
            "iex",
        )

    def test_source_does_not_change_event_id(self):
        common = {
            "symbol": "MSFT",
            "trade_id": 99,
            "price": 420.0,
            "size": 10,
            "event_timestamp": (
                "2026-09-21T14:30:00+00:00"
            ),
            "exchange": "V",
            "conditions": ["@"],
            "tape": "C",
            "feed": "iex",
        }

        live = build_trade_event(
            **common,
            source="live",
        )

        historical = build_trade_event(
            **common,
            source="historical",
        )

        self.assertEqual(
            live["event_id"],
            historical["event_id"],
        )

    def test_feed_changes_event_id(self):
        common = {
            "symbol": "MSFT",
            "trade_id": 99,
            "price": 420.0,
            "size": 10,
            "event_timestamp": (
                "2026-09-21T14:30:00+00:00"
            ),
            "exchange": "V",
            "conditions": ["@"],
            "tape": "C",
            "source": "historical",
        }

        iex = build_trade_event(
            **common,
            feed="iex",
        )

        sip = build_trade_event(
            **common,
            feed="sip",
        )

        self.assertNotEqual(
            iex["event_id"],
            sip["event_id"],
        )

    def test_trade_id_can_repeat_on_different_dates(self):
        common = {
            "symbol": "AMZN",
            "trade_id": 16,
            "price": 248.30,
            "size": 15,
            "exchange": "V",
            "conditions": ["@", "I"],
            "tape": "C",
            "source": "historical",
            "feed": "iex",
        }

        first_session = build_trade_event(
            **common,
            event_timestamp=(
                "2026-09-16T13:30:00.095972+00:00"
            ),
        )

        second_session = build_trade_event(
            **common,
            event_timestamp=(
                "2026-09-17T13:00:00.217793+00:00"
            ),
        )

        self.assertEqual(
            first_session["event_id"],
            "alpaca:iex:AMZN:2026-09-16:16",
        )
        self.assertEqual(
            second_session["event_id"],
            "alpaca:iex:AMZN:2026-09-17:16",
        )
        self.assertNotEqual(
            first_session["event_id"],
            second_session["event_id"],
        )


if __name__ == "__main__":
    unittest.main()
