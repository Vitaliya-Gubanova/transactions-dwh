from producer import AMOUNT_RANGE, MERCHANTS, generate_transaction


def test_transaction_has_all_fields():
    tx = generate_transaction()
    assert set(tx) == {
        "transaction_id",
        "client_id",
        "merchant",
        "category",
        "amount",
        "currency",
        "event_time",
    }


def test_merchant_matches_category():
    for _ in range(200):
        tx = generate_transaction()
        assert tx["merchant"] in MERCHANTS[tx["category"]]


def test_amount_in_range_or_negative_brak():
    for _ in range(500):
        tx = generate_transaction()
        low, high = AMOUNT_RANGE[tx["category"]]
        assert low <= abs(tx["amount"]) <= high


def test_ids_are_unique():
    ids = {generate_transaction()["transaction_id"] for _ in range(1000)}
    assert len(ids) == 1000
