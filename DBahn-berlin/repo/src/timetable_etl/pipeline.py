from __future__ import annotations

from .config import Settings
from .stations import import_stationen
from .stops_planned import import_stops_from_archives
from .stops_changed import process_change_archives

def run_task1(conn, settings: Settings) -> dict:
    """Run the full Task 1 pipeline: stations -> planned stops -> changed stops."""
    stations_upserted = import_stationen(conn, settings.station_json_path)

    planned_res = import_stops_from_archives(
        conn,
        settings.planned_archives_path,
        pattern=settings.archive_pattern,
        timezone=settings.timezone,
        match_threshold=settings.match_threshold,
        ambiguity_delta=settings.ambiguity_delta,
        batch_size=settings.planned_batch_size,
    )

    changed_res = process_change_archives(
        conn,
        settings.changes_archives_path,
        pattern=settings.archive_pattern,
        batch_size=settings.change_batch_size,
    )

    return {
        "stations_upserted": stations_upserted,
        "planned": planned_res,
        "changed": changed_res,
    }
