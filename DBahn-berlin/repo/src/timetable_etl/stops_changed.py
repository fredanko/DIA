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
    """Ensure schema elements required for change processing exist."""
    ensure_dimensions(conn)
    # stops table is created in planned pipeline; but keep constraint idempotent
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/constraint_stops_cs_check.sql"))
    conn.commit()


def parse_db_time(x: str | None) -> datetime | None:
    """Parse DB time format YYMMDDHHMM (Europe/Berlin)."""
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
    """Expect member paths like: '2510011345/..._change.xml' -> snapshot_ts."""
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
    str,                 # stop_id
    Optional[datetime],  # snapshot_ts
    Optional[datetime],  # arrival_ct
    Optional[datetime],  # departure_ct
    Optional[datetime],  # arrival_clt
    Optional[datetime],  # departure_clt
    Optional[str],       # arrival_cs
    Optional[str],       # departure_cs
    Optional[str],       # arrival_cp
    Optional[str],       # departure_cp
]


def merge_rows(a: Row, b: Row) -> Row:
    """Merge two rows with same stop_id and same snapshot: prefer non-null values."""
    # both have same stop_id
    return (
        a[0],
        a[1] or b[1],
        a[2] or b[2],
        a[3] or b[3],
        a[4] or b[4],
        a[5] or b[5],
        a[6] or b[6],
        a[7] or b[7],
        a[8] or b[8],
        a[9] or b[9],
    )


def apply_batch(conn, batch_rows: List[Row]) -> int:
    """Stage changes and apply latest snapshot per stop_id."""
    if not batch_rows:
        return 0

    if execute_values is None:  # pragma: no cover
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

        # 1) Update latest snapshot per stop_id
        cur.execute(load_sql("dml/upsert_latest_snapshot_from_stage.sql"))

        # 2) Apply only if the staged row matches the latest snapshot
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
    """Process _change.xml archives and update ct/clt/cs/cp fields in stops."""
    ensure_change_support(conn)
    init_stage_tables(conn)

    updated_total = 0
    files = 0

    # Dedupe per stop_id within a batch: max(snapshot_ts) wins
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
            if existing is None:
                batch[stop_id] = row
            else:
                # newer snapshot wins; if equal snapshot, merge non-null fields
                if snap_or_min(row[1]) > snap_or_min(existing[1]):
                    batch[stop_id] = row
                elif snap_or_min(row[1]) == snap_or_min(existing[1]):
                    batch[stop_id] = merge_rows(existing, row)

            if len(batch) >= batch_size:
                flush()

    flush()

    return {
        "archives_processed": files,
        "stops_updated": updated_total,
    }
