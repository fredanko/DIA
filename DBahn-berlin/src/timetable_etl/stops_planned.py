from __future__ import annotations
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Iterable
from collections import defaultdict, Counter

try:
    from psycopg2.extras import execute_values
except Exception:
    execute_values = None

from .sql import load_sql
from .io_archives import iter_xml_roots
from .station_matching import (
    StationRec,
    normalize_name,
    token_set,
    build_token_index,
    best_station_match,
)
from .dimensions import ensure_dimensions, get_time_ids, get_train_ids, TrainKey

_pt_re = re.compile(r"^\d{10}$")  # YYMMDDHHMM


# Make sure the the stops table exist
def ensure_stops_table(conn) -> None:
    ensure_dimensions(conn)
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_stops.sql"))
        cur.execute(load_sql("schema/constraint_stops_cs_check.sql"))
    conn.commit()


def parse_pt(pt: str | None, tz: ZoneInfo) -> datetime | None:
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
    except Exception:
        return None

# Helper function to read one XML file
def extract_station_and_rows(root: ET.Element) -> tuple[str, list[dict]] | None:
    station_name = root.get("station")
    if not station_name:
        return None

    rows: list[dict] = []
    for s in root.findall("s"):
        stop_id = s.get("id")
        if not stop_id:
            continue

        ar = s.find("ar")
        dp = s.find("dp")
        tl = s.find("tl")

        arrival_pt = ar.get("pt") if ar is not None else None
        departure_pt = dp.get("pt") if dp is not None else None

        arrival_pp = ar.get("pp") if ar is not None else None
        departure_pp = dp.get("pp") if dp is not None else None

        line = None
        if dp is not None:
            line = dp.get("l")
        if line is None and ar is not None:
            line = ar.get("l")

        owner = tl.get("o") if tl is not None else None
        category = tl.get("c") if tl is not None else None
        number = tl.get("n") if tl is not None else None
        trip_type = tl.get("t") if tl is not None else None
        filter_flags = tl.get("f") if tl is not None else None

        train_key: TrainKey | None = None
        if owner and category and number:
            train_key = (owner, category, number, trip_type, filter_flags, line)

        rows.append(
            {
                "stop_id": stop_id,
                "arrival_pt": arrival_pt,
                "departure_pt": departure_pt,
                "arrival_pp": arrival_pp,
                "departure_pp": departure_pp,
                "train_key": train_key,
            }
        )

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
        stations.append(StationRec(eva=int(eva), name=str(name), norm=norm, toks=toks))
    return stations

def _mode(values: Iterable, *, prefer_non_null: bool = True):
    vals = list(values)
    if not vals:
        return None

    c = Counter(vals)
    top_count = max(c.values())
    winners = [v for v, n in c.items() if n == top_count]
    if len(winners) == 1:
        return winners[0]

    if prefer_non_null:
        non_null = [v for v in winners if v is not None]
        if non_null:
            winners = non_null
            if len(winners) == 1:
                return winners[0]

    winners.sort(key=lambda x: "" if x is None else str(x))
    return winners[0]


def majority_dedup_batch(batch: list[tuple]) -> list[tuple]:
    by_stop: dict[str, list[tuple]] = defaultdict(list)
    for row in batch:
        by_stop[row[0]].append(row)

    out: list[tuple] = []
    for stop_id, rows in by_stop.items():
        evas = [r[1] for r in rows]
        train_ids = [r[2] for r in rows]
        arr_ids = [r[3] for r in rows]
        dep_ids = [r[4] for r in rows]
        arr_pps = [r[5] for r in rows]
        dep_pps = [r[6] for r in rows]

        out.append(
            (
                stop_id,
                _mode(evas),
                _mode(train_ids),
                _mode(arr_ids, prefer_non_null=True),
                _mode(dep_ids, prefer_non_null=True),
                _mode(arr_pps, prefer_non_null=True),
                _mode(dep_pps, prefer_non_null=True),
            )
        )

    return out


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
    ensure_stops_table(conn)

    if execute_values is None:
        raise RuntimeError("psycopg2.extras.execute_values not available")

    tz = ZoneInfo(timezone)

    stations = load_stations(conn)
    token_index = build_token_index(stations)

    inserted = 0
    files = 0
    unmatched_stations: set[str] = set()

    staged: list[dict] = []

    def flush() -> None:
        nonlocal inserted, staged
        if not staged:
            return

        # Resolve time ids
        all_times: list[datetime] = []
        for r in staged:
            if r["arrival_pt_dt"] is not None:
                all_times.append(r["arrival_pt_dt"])
            if r["departure_pt_dt"] is not None:
                all_times.append(r["departure_pt_dt"])
        time_ids = get_time_ids(conn, all_times)

        # Resolve train ids
        train_keys = [r["train_key"] for r in staged if r["train_key"] is not None]
        train_ids = get_train_ids(conn, train_keys)

        batch: list[tuple] = []
        for r in staged:
            train_key = r["train_key"]
            train_id = train_ids.get(train_key)
            if train_id is None:
                continue

            arrival_pt_id = time_ids.get(r["arrival_pt_dt"]) if r["arrival_pt_dt"] is not None else None
            departure_pt_id = time_ids.get(r["departure_pt_dt"]) if r["departure_pt_dt"] is not None else None

            batch.append(
                (
                    r["stop_id"],
                    r["eva"],
                    train_id,
                    arrival_pt_id,
                    departure_pt_id,
                    r["arrival_pp"],
                    r["departure_pp"],
                )
            )
        
        # Majority Vote for Deduplication
        batch = majority_dedup_batch(batch)

        with conn.cursor() as cur:
            execute_values(
                cur,
                load_sql("dml/upsert_stops_execute_values.sql"),
                batch,
                page_size=2000,
            )
        conn.commit()
        inserted += len(batch)
        staged = []

    archives_dir = Path(archives_dir)

    for _, _, root in iter_xml_roots(archives_dir, pattern=pattern):
        files += 1
        extracted = extract_station_and_rows(root)
        if extracted is None:
            continue

        station_name, rows = extracted
        eva, score, matched_name, is_ambiguous = best_station_match(
            station_name, stations, token_index, match_threshold, ambiguity_delta
        )

        if eva is None:
            unmatched_stations.add(station_name)
            continue

        for r in rows:
            arrival_pt_dt = parse_pt(r["arrival_pt"], tz)
            departure_pt_dt = parse_pt(r["departure_pt"], tz)

            staged.append(
                {
                    "stop_id": r["stop_id"],
                    "eva": eva,
                    "arrival_pt_dt": arrival_pt_dt,
                    "departure_pt_dt": departure_pt_dt,
                    "arrival_pp": r["arrival_pp"],
                    "departure_pp": r["departure_pp"],
                    "train_key": r["train_key"],
                }
            )

            if len(staged) >= batch_size:
                flush()

    flush()

    return {
        "archives_processed": files,
        "stops_upserted": inserted,
        "unmatched_stations": sorted(unmatched_stations),
    }
