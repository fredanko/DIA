from __future__ import annotations

from pathlib import Path
import json
from typing import Iterable

from .sql import load_sql

def ensure_stationen_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_stationen.sql"))
    conn.commit()

def iter_station_rows(station_json_path: str | Path) -> Iterable[tuple[int, str, float | None, float | None]]:
    station_json_path = Path(station_json_path)

    raw = json.loads(station_json_path.read_text(encoding="utf-8"))
    stations = raw.get("result") if isinstance(raw, dict) and "result" in raw else raw
    if not isinstance(stations, list):
        return

    for st in stations:
        if not isinstance(st, dict):
            continue
        name = st.get("name")
        if not name:
            continue

        for eva in (st.get("evaNumbers") or []):
            if not isinstance(eva, dict):
                continue
            if eva.get("isMain") is not True:
                continue

            eva_no = eva.get("number")
            if eva_no is None:
                continue

            lon = lat = None
            coords = (eva.get("geographicCoordinates") or {}).get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                # common order is [lon, lat]
                try:
                    lon = float(coords[0]) if coords[0] is not None else None
                    lat = float(coords[1]) if coords[1] is not None else None
                except (TypeError, ValueError):
                    lon = lat = None

            try:
                yield (int(eva_no), str(name), lat, lon)
            except (TypeError, ValueError):
                continue

def upsert_station_rows(conn, rows: Iterable[tuple[int, str, float | None, float | None]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(load_sql("dml/upsert_stationen.sql"), rows)
    conn.commit()
    return len(rows)

def import_stationen(conn, station_json_path: str | Path) -> int:
    ensure_stationen_table(conn)
    return upsert_station_rows(conn, iter_station_rows(station_json_path))