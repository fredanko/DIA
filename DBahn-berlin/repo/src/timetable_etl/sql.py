from __future__ import annotations

from functools import lru_cache
from pathlib import Path

def repo_root() -> Path:
    # repo_root / src / timetable_etl / sql.py  -> parents[2] == repo_root
    return Path(__file__).resolve().parents[2]

SQL_ROOT = repo_root() / "sql"

@lru_cache(maxsize=256)
def load_sql(relative_path: str) -> str:
    """Load a .sql file from the repository's sql/ folder."""
    path = (SQL_ROOT / relative_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")
