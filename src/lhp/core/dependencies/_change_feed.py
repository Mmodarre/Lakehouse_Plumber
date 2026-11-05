"""Whether a delta load reads its table's change feed.

A change-feed read (``options.readChangeFeed``) streams the table's change
events rather than its rows, so it follows tables whose rows are rewritten.
The delta load generator (its incompatible-option and batch-bound checks) and
the REPLACE WHERE stream-read check (LHP-VAL-072) both decide it here.
"""

from typing import Mapping


def is_change_feed_read(options: Mapping[str, object]) -> bool:
    """Whether delta reader ``options`` turn on the change feed.

    ``readChangeFeed`` set to the boolean ``True`` or to the string ``"true"``
    in any case, which is how Spark parses the option. Other values, and an
    absent key, do not.
    """
    value = options.get("readChangeFeed")
    if isinstance(value, str):
        return value.casefold() == "true"
    return value is True
