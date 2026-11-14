"""Unit tests for the Bonus stream-to-lakehouse bridge."""
import gzip
import json
import tempfile
from datetime import date
from pathlib import Path

from stream.stream_to_lakehouse import format_freshcart_order, write_stream_landing


def test_format_freshcart_order():
    raw_event = {
        "id": 105,
        "city": "bangalore",
        "zone": "koramangala",
        "type": "food",
        "ts": "2026-09-30T10:15:00Z"
    }
    order = format_freshcart_order(raw_event)
    assert order["order_id"] == "ORD-STRM-0105"
    assert order["pincode"] == "560034"
    assert order["currency"] == "INR"
    assert order["amount"] == "1,500.00"
    assert order["order_date"] == "2026-09-30"


def test_write_stream_landing():
    sample_orders = [
        {"order_id": "ORD-STRM-0001", "amount": "1,500.00", "currency": "INR"},
        {"order_id": "ORD-STRM-0002", "amount": "450.00", "currency": "INR"}
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        path = write_stream_landing(sample_orders, "test_run", date(2026, 9, 30), landing_dir=tmpdir)
        assert path.exists()
        assert path.name == "stream__test_run.json.gz"

        # Verify compressed contents match Snowflake bronze expectation
        with gzip.open(path, "rt", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f]
            assert len(lines) == 2
            assert lines[0]["source"] == "web"
            assert lines[0]["data"]["order_id"] == "ORD-STRM-0001"
            assert lines[1]["data"]["order_id"] == "ORD-STRM-0002"
