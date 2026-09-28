import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


AIRFLOW_DAGS = (
    Path(__file__).resolve().parents[1]
    / "airflow"
    / "dags"
)

sys.path.insert(0, str(AIRFLOW_DAGS))

from backfill_validation import (  # noqa: E402
    build_expected_windows,
    compare_windows,
    floor_timestamp,
)


class TestBackfillValidation(unittest.TestCase):

    def test_floor_timestamp(self):
        timestamp = datetime(
            2026,
            9,
            25,
            13,
            37,
            42,
            tzinfo=timezone.utc,
        )

        self.assertEqual(
            floor_timestamp(timestamp, 1),
            datetime(
                2026,
                9,
                25,
                13,
                37,
                tzinfo=timezone.utc,
            ),
        )

        self.assertEqual(
            floor_timestamp(timestamp, 5),
            datetime(
                2026,
                9,
                25,
                13,
                35,
                tzinfo=timezone.utc,
            ),
        )

        self.assertEqual(
            floor_timestamp(timestamp, 15),
            datetime(
                2026,
                9,
                25,
                13,
                30,
                tzinfo=timezone.utc,
            ),
        )

    def test_build_expected_windows_from_trades(self):
        events = [
            {
                "symbol": "MSFT",
                "event_timestamp": (
                    "2026-09-25T13:31:10Z"
                ),
            },
            {
                "symbol": "MSFT",
                "event_timestamp": (
                    "2026-09-25T13:37:20Z"
                ),
            },
            {
                "symbol": "MSFT",
                "event_timestamp": (
                    "2026-09-25T13:37:50Z"
                ),
            },
        ]

        expected = build_expected_windows(
            events,
            ["MSFT"],
        )

        self.assertEqual(
            len(expected["MSFT"]["1m"]),
            2,
        )
        self.assertEqual(
            len(expected["MSFT"]["5m"]),
            2,
        )
        self.assertEqual(
            len(expected["MSFT"]["15m"]),
            1,
        )

    def test_complete_reconstruction_has_no_differences(self):
        expected = {
            datetime(
                2026,
                9,
                25,
                13,
                minute,
                tzinfo=timezone.utc,
            )
            for minute in range(30, 40)
        }

        missing, unexpected = compare_windows(
            expected,
            expected.copy(),
        )

        self.assertEqual(missing, set())
        self.assertEqual(unexpected, set())

    def test_sparse_reconstruction_detects_missing_windows(self):
        expected = {
            datetime(
                2026,
                9,
                25,
                13,
                minute,
                tzinfo=timezone.utc,
            )
            for minute in range(30, 40)
        }

        actual = {
            datetime(
                2026,
                9,
                25,
                13,
                minute,
                tzinfo=timezone.utc,
            )
            for minute in (30, 35)
        }

        missing, unexpected = compare_windows(
            expected,
            actual,
        )

        self.assertEqual(len(expected), 10)
        self.assertEqual(len(actual), 2)
        self.assertEqual(len(missing), 8)
        self.assertEqual(unexpected, set())

    def test_unexpected_windows_are_detected(self):
        expected = {
            datetime(
                2026,
                9,
                25,
                13,
                30,
                tzinfo=timezone.utc,
            ),
        }

        unexpected_window = datetime(
            2026,
            9,
            25,
            13,
            31,
            tzinfo=timezone.utc,
        )

        actual = expected | {unexpected_window}

        missing, unexpected = compare_windows(
            expected,
            actual,
        )

        self.assertEqual(missing, set())
        self.assertEqual(
            unexpected,
            {unexpected_window},
        )

    def test_unexpected_symbol_is_rejected(self):
        events = [
            {
                "symbol": "AAPL",
                "event_timestamp": (
                    "2026-09-25T13:30:00Z"
                ),
            },
        ]

        with self.assertRaisesRegex(
            ValueError,
            "unexpected symbol",
        ):
            build_expected_windows(
                events,
                ["MSFT"],
            )


if __name__ == "__main__":
    unittest.main()
