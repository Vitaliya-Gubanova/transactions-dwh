import json
from datetime import datetime
from decimal import Decimal

from consumer import parse_message

VALID = {
    "transaction_id": "abc-123",
    "client_id": 42,
    "merchant": "Magnit",
    "category": "food",
    "amount": 350.5,
    "currency": "RUB",
    "event_time": "2026-11-08T12:00:00.123+00:00",
}


def test_parse_valid_message():
    row = parse_message(json.dumps(VALID))
    assert row[0] == "abc-123"
    assert row[1] == 42
    assert row[4] == Decimal("350.5")
    assert isinstance(row[6], datetime)


def test_parse_broken_json():
    assert parse_message(b"not a json") is None


def test_parse_missing_field():
    data = dict(VALID)
    del data["amount"]
    assert parse_message(json.dumps(data)) is None


def test_parse_bad_date():
    data = dict(VALID, event_time="вчера")
    assert parse_message(json.dumps(data)) is None
