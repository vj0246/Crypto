"""Daily-partitioned Parquet writer.

One writer per logical table. Rows are buffered in memory and flushed on a
size or time trigger; each UTC day gets its own file so a crashed recorder
loses at most one flush interval and never corrupts earlier days.
"""
from __future__ import annotations

import datetime as dt
import threading
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from cascade.schema import SCHEMAS

NS_PER_DAY = 86_400_000_000_000


def day_of(ts_ns: int) -> str:
    return dt.datetime.fromtimestamp(ts_ns / 1e9, dt.timezone.utc).strftime("%Y-%m-%d")


class ParquetWriter:
    """Append-only buffered writer. Thread-safe."""

    def __init__(
        self,
        table: str,
        root: Path,
        max_rows: int = 20_000,
        max_age_s: float = 30.0,
    ) -> None:
        if table not in SCHEMAS:
            raise KeyError(f"unknown table {table!r}; known: {sorted(SCHEMAS)}")
        self.table = table
        self.schema = SCHEMAS[table]
        self.root = Path(root) / table
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_rows = max_rows
        self.max_age_s = max_age_s
        self._buf: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = dt.datetime.now(dt.timezone.utc).timestamp()
        self._written = 0

    @property
    def rows_written(self) -> int:
        return self._written

    def append(self, row: dict[str, Any]) -> None:
        with self._lock:
            self._buf.append(row)
            due = self._is_due_locked()
        if due:
            self.flush()

    def extend(self, rows: Iterable[dict[str, Any]]) -> None:
        with self._lock:
            self._buf.extend(rows)
            due = self._is_due_locked()
        if due:
            self.flush()

    def _is_due_locked(self) -> bool:
        if len(self._buf) >= self.max_rows:
            return True
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        return bool(self._buf) and (now - self._last_flush) >= self.max_age_s

    def flush(self) -> int:
        with self._lock:
            if not self._buf:
                return 0
            buf, self._buf = self._buf, []
            self._last_flush = dt.datetime.now(dt.timezone.utc).timestamp()

        # Split by UTC day so a flush spanning midnight lands in both files.
        by_day: dict[str, list[dict[str, Any]]] = {}
        for r in buf:
            by_day.setdefault(day_of(r["ts_ns"]), []).append(r)

        n = 0
        for day, rows in by_day.items():
            tbl = pa.Table.from_pylist(rows, schema=self.schema)
            path = self.root / f"{day}.parquet"
            self._append_file(path, tbl)
            n += tbl.num_rows
        self._written += n
        return n

    def _append_file(self, path: Path, tbl: pa.Table) -> None:
        """Parquet has no true append; concatenate via a temp rewrite.

        Fine at flush cadence (files stay well under a day of events); the
        offline normalizer compacts them afterwards.
        """
        if path.exists():
            existing = pq.read_table(path, schema=self.schema)
            tbl = pa.concat_tables([existing, tbl])
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(tbl, tmp, compression="zstd")
        tmp.replace(path)

    def close(self) -> int:
        return self.flush()

    def __enter__(self) -> "ParquetWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
