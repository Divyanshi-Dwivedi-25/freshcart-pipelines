"""AMQP topic-pattern matching, mirroring the broker (used for tests and D1 checks)."""
from functools import lru_cache


def topic_matches(pattern: str, key: str) -> bool:
    """'*' matches exactly one word, '#' matches zero or more words."""
    p, k = pattern.split("."), key.split(".")

    @lru_cache(None)
    def rec(i, j):
        if i == len(p):
            return j == len(k)
        if p[i] == "#":
            return rec(i + 1, j) or (j < len(k) and rec(i, j + 1))
        return j < len(k) and p[i] in ("*", k[j]) and rec(i + 1, j + 1)

    return rec(0, 0)
