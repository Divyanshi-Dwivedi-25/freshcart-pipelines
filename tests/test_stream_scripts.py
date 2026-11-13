from stream.dispatcher import BAD_EVERY, build_request
from stream.driver import is_valid


def test_every_tenth_message_is_bad():
    for i in range(1, 41):
        _, msg = build_request(i)
        assert ("zone" not in msg) == (i % BAD_EVERY == 0)


def test_good_message_has_required_fields_and_matching_key():
    key, msg = build_request(1)
    assert set(msg) == {"id", "city", "zone", "type", "ts"}
    assert key == f"{msg['city']}.{msg['zone']}.{msg['type']}"


def test_driver_validation():
    assert is_valid({"id": 1, "zone": "tnagar"})
    assert not is_valid({"id": 10})
