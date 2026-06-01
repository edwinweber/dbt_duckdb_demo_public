"""String and date utilities for the Danish Democracy Data project.

Contains helpers that transform or normalise strings and dates used across
extraction, transformation, and export layers.
"""

from datetime import datetime, timedelta


def normalize_danish_name(name: str) -> str:
    """Convert a Danish entity name to a filesystem-safe ASCII identifier.

    Replaces all six Danish characters (upper- and lowercase) that are
    unsupported in DuckDB schema names, dbt model names, and OneLake / local
    file-system paths: Ø/ø → oe, Æ/æ → ae, Å/å → aa.  The result is
    lowercased.  Lowercasing is applied first so that a single set of
    replacements covers both cases.

    This is the single canonical implementation; every other module that
    needs this normalisation imports and calls this function.
    """
    return (
        name.lower()
        .replace("ø", "oe")
        .replace("æ", "ae")
        .replace("å", "aa")
    )


def resolve_date_to_load_from(
    date_to_load_from: str | None,
    default_days: int,
    reference_time: datetime,
) -> str:
    """Validate or compute a ``YYYY-MM-DD`` date string for incremental extraction.

    If *date_to_load_from* is ``None`` the default lookback window is applied
    (``reference_time - default_days``).  Otherwise the supplied string is
    validated and returned unchanged.

    Args:
        date_to_load_from: Caller-supplied date string, or ``None`` to use the
            default lookback window.
        default_days: Number of days to subtract from *reference_time* when
            computing the default lower bound.
        reference_time: The timestamp to measure the lookback from (typically
            the script start time so logging is consistent).

    Returns:
        A ``YYYY-MM-DD`` date string.

    Raises:
        ValueError: If *date_to_load_from* is provided but does not match
            ``YYYY-MM-DD``.
    """
    if date_to_load_from is None:
        return f"{reference_time - timedelta(days=default_days):%Y-%m-%d}"
    try:
        datetime.strptime(date_to_load_from, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"date_to_load_from '{date_to_load_from}' must be in 'YYYY-MM-DD' format."
        )
    return date_to_load_from
