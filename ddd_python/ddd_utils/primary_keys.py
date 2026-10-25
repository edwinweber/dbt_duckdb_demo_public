"""Primary keys of one or more columns.

A table's primary key in ``configuration_variables`` is a column name, or a list of
column names for a key of several columns.  A key of several columns is stored as one
character column, :data:`PRIMARY_KEY_COLUMN`: the Bronze view computes it with
``generate_primary_key_column`` (the same concatenation, NULL token and delimiter as the
change hash), the Silver table stores it, and the Silver CDC uses it as its key.  A key
of one column is used as it is.
"""

from __future__ import annotations

PRIMARY_KEY_COLUMN = "LKHS_primary_key_1_column"

PrimaryKey = str | list[str]


def key_columns(primary_key: PrimaryKey) -> list[str]:
    """The source columns of a key, in order."""
    columns = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    if not columns or not all(isinstance(c, str) and c for c in columns):
        raise ValueError(f"A primary key needs at least one column name, got {primary_key!r}")
    return columns


def is_composite(primary_key: PrimaryKey) -> bool:
    """True for a key of more than one column."""
    return len(key_columns(primary_key)) > 1


def stored_key_column(primary_key: PrimaryKey) -> str:
    """The single column Silver uses as key: the column itself, or PRIMARY_KEY_COLUMN."""
    columns = key_columns(primary_key)
    return PRIMARY_KEY_COLUMN if len(columns) > 1 else columns[0]
