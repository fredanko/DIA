# config.py
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

@dataclass(frozen=True)
class DBConfig:
    host: str = "localhost"
    dbname: str = "postgres"
    user: str = "postgres"
    password: str = "1234"

@dataclass(frozen=True)
class PathConfig:
    timetables_dir: Path = PROJECT_ROOT / "timetables"
    timetable_changes_dir: Path = PROJECT_ROOT / "timetable_changes"
    station_json: Path = PROJECT_ROOT / "station_data.json"
    sqls: Path = PROJECT_ROOT / "sql"

@dataclass(frozen=True)
class MiscConfig:
    timezone = "Europe/Berlin"

@dataclass(frozen=True)
class StringMatchtingConfig:
    match_treshold = 0.85
    ambiguity_delta = 0.02
    batch_size = 5000

@dataclass(frozen=True)
class Config:
    db = DBConfig()
    paths = PathConfig()
    misc = MiscConfig()

cfg = Config()