from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Iterable

try:
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover
    execute_values = None

from .sql import load_sql
from .io_archives import iter_xml_roots
from .station_matching import StationRec, normalize_name, token_set, build_token_index, best_station_match

_pt_re = re.compile(r"^\d{10}$")  # YYMMDDHHMM

def ensure_stops_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_stops.sql"))
    conn.commit()

def parse_pt(pt: str | None, tz: ZoneInfo) -> datetime | None:
    """Parse planned time 'YYMMDDHHMM' into an aware datetime."""
    if pt is None:
        return None
    pt = pt.strip()
    if not _pt_re.match(pt):
        return None

    try:
        year = 2000 + int(pt[0:2])
        month = int(pt[2:4])
        day = int(pt[4:6])
        hour = int(pt[6:8])
        minute = int(pt[8:10])
        return datetime(year, month, day, hour, minute, tzinfo=tz)
    except ValueError:
        return None

def extract_s_id_and_pts(root: ET.Element):
    """Return (station_name, rows) where rows contains s/@id and ar/dp pt values."""
    station_name = root.get("station", None)
    if not station_name:
        return None
    rows = []
    for s in root.findall("s"):
        s_id = s.get("id")

        ar = s.find("ar")
        dp = s.find("dp")

        ar_pt = ar.get("pt") if ar is not None else None
        dp_pt = dp.get("pt") if dp is not None else None

        rows.append({"id": s_id, "ar_pt": ar_pt, "dp_pt": dp_pt})
    return station_name, rows

def load_stations(conn) -> list[StationRec]:
    with conn.cursor() as cur:
        cur.execute(load_sql("queries/select_stations.sql"))
        rows = cur.fetchall()

    stations: list[StationRec] = []
    for eva, name in rows:
        if not name:
            continue
        norm = normalize_name(name)
        toks = tuple(sorted(token_set(norm)))
        stations.append(StationRec(int(eva), name, norm, toks))
    return stations

def flush_batch(conn, batch: list[tuple], prefer_execute_values: bool = True) -> int:
    if not batch:
        return 0

    with conn.cursor() as cur:
        if prefer_execute_values and execute_values is not None:
            execute_values(cur, load_sql("dml/upsert_stops_execute_values.sql"), batch, page_size=2000)
        else:
            cur.executemany(load_sql("dml/upsert_stops_executemany.sql"), batch)

    conn.commit()
    return len(batch)

def import_stops_from_archives(
    conn,
    archives_dir: str | Path,
    *,
    pattern: str = "*.tar.gz",
    timezone: str = "Europe/Berlin",
    match_threshold: float = 0.85,
    ambiguity_delta: float = 0.02,
    batch_size: int = 5000,
) -> dict:
    """Import planned stops into the `stops` table."""
    ensure_stops_table(conn)

    tz = ZoneInfo(timezone)

    stations = load_stations(conn)
    token_index = build_token_index(stations)

    unmatched_station_names: set[str] = set()
    ambiguous_station_names: set[str] = set()

    match_cache: dict[str, tuple[int | None, bool]] = {}  # name -> (eva_or_None, ambiguous)

    # dedupe within a batch on stop_id
    batch: dict[str, tuple] = {}  # stop_id -> row

    total_seen_stops = 0
    total_upserted = 0

    for _, _, root in iter_xml_roots(archives_dir, pattern=pattern):
        res = extract_s_id_and_pts(root)
        if not res:
            continue

        station_name, stop_rows = res
        if not station_name:
            continue

        if station_name in match_cache:
            eva, is_ambiguous = match_cache[station_name]
        else:
            eva, score, matched_name, is_ambiguous = best_station_match(
                station_name,
                stations,
                token_index,
                threshold=match_threshold,
                ambiguity_delta=ambiguity_delta,
            )
            match_cache[station_name] = (eva, is_ambiguous)

        if eva is None:
            unmatched_station_names.add(station_name)
            continue
        if is_ambiguous:
            ambiguous_station_names.add(station_name)
            continue

        for r in stop_rows:
            stop_id = r.get("id")
            if not stop_id:
                continue

            ar_ts = parse_pt(r.get("ar_pt"), tz)
            dp_ts = parse_pt(r.get("dp_pt"), tz)

            batch[stop_id] = (stop_id, eva, ar_ts, dp_ts)
            total_seen_stops += 1

            if len(batch) >= batch_size:
                total_upserted += flush_batch(conn, list(batch.values()), prefer_execute_values=True)
                batch.clear()

    total_upserted += flush_batch(conn, list(batch.values()), prefer_execute_values=True)
    batch.clear()

    return {
        "total_seen_stops": total_seen_stops,
        "total_upserted": total_upserted,
        "unmatched_station_names": unmatched_station_names,
        "ambiguous_station_names": ambiguous_station_names,
        "match_cache_size": len(match_cache),
    }
