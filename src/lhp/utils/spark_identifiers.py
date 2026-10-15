"""Quote column names for Spark APIs that parse them as column references."""

from __future__ import annotations

from typing import Any


def spark_column_ref(name: Any) -> str:
    """Return ``name`` spelled so ``F.col`` and ``DataFrame.select`` resolve that column.

    Those APIs parse a string argument: ``.`` selects a struct field, a
    backtick opens a quoted part and a bare ``*`` means every column. A name
    containing ``.`` or a backtick, or equal to ``*``, is wrapped in backticks
    with each inner backtick doubled. Any other name is returned unchanged, so
    code for names that already resolved stays byte-identical.

    ``withColumnRenamed`` and the name argument of ``withColumn`` take names
    literally; a backtick-quoted name there matches nothing.
    """
    text = str(name)
    if text != "*" and "." not in text and "`" not in text:
        return text
    escaped = text.replace("`", "``")
    return f"`{escaped}`"
