"""Table writers for feature exports."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import polars as pl


def write_table(
    rows: Iterable[Mapping[str, object]],
    output_stem: Path,
    formats: Sequence[str],
    columns: Optional[Sequence[str]] = None,
) -> Tuple[Path, ...]:
    """Write rows to CSV and optional Parquet using stable columns."""

    output_stem = Path(output_stem)
    output_stem.parent.mkdir(parents=True, exist_ok=True)
    row_list = [dict(row) for row in rows]

    if columns is None and not row_list:
        raise ValueError("columns are required when writing an empty table")

    if columns is None:
        ordered_columns = list(row_list[0].keys())
    else:
        ordered_columns = list(columns)

    written: List[Path] = []
    for fmt in formats:
        if fmt == "csv":
            path = output_stem.with_suffix(".csv")
            _write_csv(path, row_list, ordered_columns)
        elif fmt == "parquet":
            path = output_stem.with_suffix(".parquet")
            _build_dataframe(row_list, ordered_columns).write_parquet(path)
        else:
            raise ValueError(f"Unsupported feature export format: {fmt}")
        written.append(path)
    return tuple(written)


def _write_csv(
    path: Path, rows: Sequence[Mapping[str, object]], columns: Sequence[str]
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _build_dataframe(
    rows: Sequence[Mapping[str, object]], columns: Sequence[str]
) -> pl.DataFrame:
    if rows:
        df = pl.DataFrame(rows)
        missing_columns = [column for column in columns if column not in df.columns]
        for column in missing_columns:
            df = df.with_columns(pl.lit(None).alias(column))
        return df.select(columns)
    return pl.DataFrame({column: [] for column in columns})
