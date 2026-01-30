from __future__ import annotations

from pathlib import Path, PurePosixPath
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, Optional, Tuple, List

try:
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover
    execute_values = None

from .sql import load_sql
from .io_archives import iter_xml_roots
from .dimensions import ensure_dimensions, get_time_ids

BERLIN_TZ = ZoneInfo("Europe/Berlin")


def ensure_change_support(conn) -> None:
    ensure_dimensions(conn)
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/constraint_stops_cs_check.sql"))
    conn.commit()


def parse_db_time(x: str | None) -> datetime | None:
    if not x:
        return None
    x = x.strip()
    if len(x) != 10 or not x.isdigit():
        return None
    dt = datetime.strptime(x, "%y%m%d%H%M")
    return dt.replace(tzinfo=BERLIN_TZ)


def parse_cs(x: str | None) -> str | None:
    if not x:
        return None
    x = x.strip().lower()
    return x if x in {"a", "p", "c"} else None


def snapshot_ts_from_member_name(xml_member_name: str) -> datetime | None:
    p = PurePosixPath(xml_member_name)
    if not p.parts:
        return None
    head = p.parts[0]
    if len(head) == 10 and head.isdigit():
        dt = datetime.strptime(head, "%y%m%d%H%M")
        return dt.replace(tzinfo=BERLIN_TZ)
    return None


def init_stage_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_temp_stops_change_stage.sql"))
        cur.execute(load_sql("schema/create_temp_stops_latest_snapshot.sql"))
    conn.commit()


Row = Tuple[
    str, # stop_id
    Optional[datetime], # snapshot_ts
    Optional[datetime], # arrival_ct
    Optional[datetime], # departure_ct
    Optional[datetime], # arrival_clt
    Optional[datetime], # departure_clt
    Optional[str], # arrival_cs
    Optional[str], # departure_cs
    Optional[str], # arrival_cp
    Optional[str], # departure_cp
]


def merge_rows_last_non_null(existing: Row, incoming: Row) -> Row:
    snap = incoming[1] if incoming[1] is not None else existing[1]
    return (
        existing[0],
        snap,
        incoming[2] if incoming[2] is not None else existing[2],
        incoming[3] if incoming[3] is not None else existing[3],
        incoming[4] if incoming[4] is not None else existing[4],
        incoming[5] if incoming[5] is not None else existing[5],
        incoming[6] if incoming[6] is not None else existing[6],
        incoming[7] if incoming[7] is not None else existing[7],
        incoming[8] if incoming[8] is not None else existing[8],
        incoming[9] if incoming[9] is not None else existing[9],
    )


def apply_batch(conn, batch_rows: List[Row]) -> int:
    if not batch_rows:
        return 0

    if execute_values is None:
        raise RuntimeError("psycopg2.extras.execute_values not available")

    init_stage_tables(conn)

    # Resolve all change timestamps to dim_time ids
    all_times: List[datetime] = []
    for r in batch_rows:
        for dt in (r[2], r[3], r[4], r[5]):
            if dt is not None:
                all_times.append(dt)
    time_ids = get_time_ids(conn, all_times)

    stage_rows: List[tuple] = []
    for r in batch_rows:
        stage_rows.append(
            (
                r[0],
                r[1],
                time_ids.get(r[2]) if r[2] is not None else None,
                time_ids.get(r[3]) if r[3] is not None else None,
                time_ids.get(r[4]) if r[4] is not None else None,
                time_ids.get(r[5]) if r[5] is not None else None,
                r[6],
                r[7],
                r[8],
                r[9],
            )
        )

    with conn.cursor() as cur:
        cur.execute(load_sql("dml/truncate_stops_change_stage.sql"))

        execute_values(
            cur,
            load_sql("dml/insert_stops_change_stage_execute_values.sql"),
            stage_rows,
            page_size=10_000,
        )

        cur.execute(load_sql("dml/upsert_latest_snapshot_from_stage.sql"))
        cur.execute(load_sql("dml/update_stops_from_stage.sql"))
        updated = cur.rowcount

    conn.commit()
    return updated


def process_change_archives(
    conn,
    archives_dir: str | Path,
    *,
    pattern: str = "*.tar.gz",
    batch_size: int = 50_000,
) -> dict:
    ensure_change_support(conn)
    init_stage_tables(conn)

    updated_total = 0
    files = 0

    # Dedupe per stop_id within a batch: max(snapshot_ts) wins, however null values do not overwrite older values
    batch: Dict[str, Row] = {}

    def snap_or_min(dt: Optional[datetime]) -> datetime:
        return dt if dt is not None else datetime.min.replace(tzinfo=BERLIN_TZ)

    def flush() -> None:
        nonlocal batch, updated_total
        if not batch:
            return
        updated_total += apply_batch(conn, list(batch.values()))
        batch = {}

    archives_dir = Path(archives_dir)

    for archive_path, member_name, root in iter_xml_roots(archives_dir, pattern=pattern):
        files += 1
        snap = snapshot_ts_from_member_name(member_name)

        for s in root.findall("s"):
            stop_id = s.get("id")
            if not stop_id:
                continue

            ar = s.find("ar")
            dp = s.find("dp")

            arrival_ct = parse_db_time(ar.get("ct")) if ar is not None else None
            departure_ct = parse_db_time(dp.get("ct")) if dp is not None else None

            arrival_clt = parse_db_time(ar.get("clt")) if ar is not None else None
            departure_clt = parse_db_time(dp.get("clt")) if dp is not None else None

            arrival_cs = parse_cs(ar.get("cs")) if ar is not None else None
            departure_cs = parse_cs(dp.get("cs")) if dp is not None else None

            arrival_cp = ar.get("cp") if ar is not None else None
            departure_cp = dp.get("cp") if dp is not None else None

            row: Row = (
                stop_id,
                snap,
                arrival_ct,
                departure_ct,
                arrival_clt,
                departure_clt,
                arrival_cs,
                departure_cs,
                arrival_cp,
                departure_cp,
            )

            existing = batch.get(stop_id)
            # if first snapshot, just accept it
            if existing is None:
                batch[stop_id] = row
            else:
                # if incoming snapshot if at the same time or later, accept newer values
                if snap_or_min(row[1]) > snap_or_min(existing[1]):
                    batch[stop_id] = merge_rows_last_non_null(existing, row)
                elif snap_or_min(row[1]) == snap_or_min(existing[1]):
                    batch[stop_id] = merge_rows_last_non_null(existing, row)
                # if snapshot is earlier, reject new values
                else:
                    pass

            if len(batch) >= batch_size:
                flush()

    flush()

    return {
        "archives_processed": files,
        "stops_updated": updated_total,
    }
