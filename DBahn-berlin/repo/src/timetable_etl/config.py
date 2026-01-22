from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os

from .sql import repo_root

def _as_path(value: str) -> Path:
    p = Path(value)
    if p.is_absolute():
        return p
    return (repo_root() / p).resolve()

@dataclass(frozen=True)
class Settings:
    # DB
    db_host: str
    db_name: str
    db_user: str
    db_password: str

    # Paths
    station_json_path: Path
    planned_archives_path: Path
    changes_archives_path: Path
    archive_pattern: str

    # Matching / batching
    timezone: str
    match_threshold: float
    ambiguity_delta: float
    planned_batch_size: int
    change_batch_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            # Read env or fallback to standard settings
            db_host=os.getenv("DB_HOST", "localhost"),
            db_name=os.getenv("DB_NAME", "postgres"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=os.getenv("DB_PASSWORD", "1234"),
            station_json_path=_as_path(os.getenv("STATION_JSON_PATH", "./station_data.json")),
            planned_archives_path=_as_path(os.getenv("PLANNED_ARCHIVES_PATH", "./timetables")),
            changes_archives_path=_as_path(os.getenv("CHANGES_ARCHIVES_PATH", "./timetable_changes")),
            archive_pattern=os.getenv("ARCHIVE_PATTERN", "*.tar.gz"),
            timezone=os.getenv("TIMEZONE", "Europe/Berlin"),
            match_threshold=float(os.getenv("MATCH_THRESHOLD", "0.75")),
            ambiguity_delta=float(os.getenv("AMBIGUITY_DELTA", "0.02")),
            planned_batch_size=int(os.getenv("PLANNED_BATCH_SIZE", "5000")),
            change_batch_size=int(os.getenv("CHANGE_BATCH_SIZE", "50000")),
        )
