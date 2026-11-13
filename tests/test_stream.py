import pytest
from stream.routing import topic_matches

DRIVERS = {
    "Ravi": "bangalore.koramangala.*", "Priya": "bangalore.indiranagar.*",
    "Arjun": "bangalore.*.ride", "Meena": "bangalore.*.food",
    "Karthik": "chennai.tnagar.food", "Control": "bangalore.#",
}

# D1 answers
CASES = {
    "bangalore.koramangala.food": {"Ravi", "Meena", "Control"},
    "bangalore.indiranagar.ride": {"Priya", "Arjun", "Control"},
    "bangalore.whitefield.ride": {"Arjun", "Control"},
    "chennai.tnagar.food": {"Karthik"},
    "chennai.whitefield.ride": set(),
    "bangalore.koramangala": {"Control"},
}


@pytest.mark.parametrize("key,expected", CASES.items())
def test_routing(key, expected):
    got = {n for n, p in DRIVERS.items() if topic_matches(p, key)}
    assert got == expected
