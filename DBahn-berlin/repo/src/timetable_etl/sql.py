from __future__ import annotations

from functools import lru_cache
from pathlib import Path

def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]

SQL_ROOT = repo_root() / "sql"

@lru_cache(maxsize=256)
def load_sql(relative_path: str) -> str:
    path = (SQL_ROOT / relative_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")
