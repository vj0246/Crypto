"""DuckDB views over the Parquet lake. Read-only query surface."""
from __future__ import annotations

from pathlib import Path

import duckdb

from cascade.config import settings
from cascade.schema import SCHEMAS

_DUCK_TYPES = {
    "int64": "BIGINT",
    "double": "DOUBLE",
    "string": "VARCHAR",
    "bool": "BOOLEAN",
}


def connect(root: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB with one view per table over its Parquet files."""
    root = Path(root or settings.events_dir)
    con = duckdb.connect()
    for table in SCHEMAS:
        glob = (root / table / "*.parquet").as_posix()
        if list((root / table).glob("*.parquet")):
            con.execute(
                f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{glob}')"
            )
        else:
            # Keep the name resolvable so queries fail on logic, not on absence.
            cols = ", ".join(
                f'CAST(NULL AS {_DUCK_TYPES[str(f.type)]}) AS "{f.name}"'
                for f in SCHEMAS[table]
            )
            con.execute(f"CREATE VIEW {table} AS SELECT {cols} WHERE FALSE")
    return con
