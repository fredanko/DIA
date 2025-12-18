from __future__ import annotations

from pathlib import Path, PurePosixPath
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

try:
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover
    execute_values = None

from .sql import load_sql
from .io_archives import iter_xml_roots

BERLIN_TZ = ZoneInfo("Europe/Berlin")

def ensure_actual_columns(conn) -> None:
    """Add columns + constraint to public.stops (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/alter_stops_add_actual_columns.sql"))
        cur.execute(load_sql("schema/constraint_stops_cs_check.sql"))
    conn.commit()

def parse_db_ct(ct: str | None):
    """Format: YYMMDDHHMM -> aware datetime (Europe/Berlin)."""
    if not ct:
        return None
    ct = ct.strip()
    if len(ct) != 10 or not ct.isdigit():
        return None
    dt = datetime.strptime(ct, "%y%m%d%H%M")
    return dt.replace(tzinfo=BERLIN_TZ)

def parse_cs(x: str | None) -> str | None:
    if not x:
        return None
    x = x.strip().lower()
    return x if x in {"a", "p", "c"} else None

def snapshot_ts_from_member_name(xml_member_name: str):
    """Expect member paths like: '2510011345/..._change.xml' -> snapshot_ts in Europe/Berlin."""
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

def apply_batch(conn, batch_rows) -> int:
    """Stage changes and apply latest snapshot per stop_id."""
    if not batch_rows:
        return 0

    init_stage_tables(conn)

    with conn.cursor() as cur:
        cur.execute(load_sql("dml/truncate_stops_change_stage.sql"))

        if execute_values is not None:
            execute_values(
                cur,
                load_sql("dml/insert_stops_change_stage_execute_values.sql"),
                batch_rows,
                page_size=10_000,
            )
        else:
            cur.executemany(
                load_sql("dml/insert_stops_change_stage_executemany.sql"),
                batch_rows,
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
    """Process _change.xml archives and update actual/cancelled times."""
    ensure_actual_columns(conn)
    init_stage_tables(conn)

    # Dedupe per stop_id within a batch: max(snapshot_ts) wins;
    # if same snapshot: non-null merge (including cs).
    batch: dict[str, tuple] = {}

    def snap_key(dt):
        return dt if dt is not None else datetime.min.replace(tzinfo=BERLIN_TZ)

    n_change_files = 0
    n_s_nodes = 0
    n_updates = 0

    for _, xml_member_name, root in iter_xml_roots(archives_dir, pattern=pattern):
        if not (xml_member_name.endswith("_change.xml") or xml_member_name.endswith("change.xml")):
            continue

        n_change_files += 1
        snapshot_ts = snapshot_ts_from_member_name(xml_member_name)

        for s in root.findall("./s"):
            stop_id = s.get("id")
            if not stop_id:
                continue

            ar = s.find("ar")
            dp = s.find("dp")

            ar_ct = ar.get("ct") if ar is not None else None
            dp_ct = dp.get("ct") if dp is not None else None
            ar_clt = ar.get("clt") if ar is not None else None
            dp_clt = dp.get("clt") if dp is not None else None

            ar_cs = parse_cs(ar.get("cs") if ar is not None else None)
            dp_cs = parse_cs(dp.get("cs") if dp is not None else None)

            actual_arrival = parse_db_ct(ar_ct)
            actual_departure = parse_db_ct(dp_ct)
            cancelled_arrival = parse_db_ct(ar_clt)
            cancelled_departure = parse_db_ct(dp_clt)

            # If there's truly no info, skip (cs counts as info).
            if (
                actual_arrival is None and actual_departure is None
                and cancelled_arrival is None and cancelled_departure is None
                and ar_cs is None and dp_cs is None
            ):
                continue

            new_row = (
                stop_id, snapshot_ts,
                actual_arrival, actual_departure,
                cancelled_arrival, cancelled_departure,
                ar_cs, dp_cs,
            )

            old = batch.get(stop_id)
            if old is None:
                batch[stop_id] = new_row
            else:
                _, old_snap, old_aa, old_ad, old_ca, old_cd, old_ar_cs, old_dp_cs = old

                new_snap_key = snap_key(snapshot_ts)
                old_snap_key = snap_key(old_snap)

                if new_snap_key > old_snap_key:
                    batch[stop_id] = new_row
                elif new_snap_key == old_snap_key:
                    # same snapshot -> non-null merge (keep existing non-null values)
                    batch[stop_id] = (
                        stop_id, old_snap,
                        old_aa if old_aa is not None else actual_arrival,
                        old_ad if old_ad is not None else actual_departure,
                        old_ca if old_ca is not None else cancelled_arrival,
                        old_cd if old_cd is not None else cancelled_departure,
                        old_ar_cs if old_ar_cs is not None else ar_cs,
                        old_dp_cs if old_dp_cs is not None else dp_cs,
                    )
                # else: older snapshot -> ignore

            n_s_nodes += 1

            if len(batch) >= batch_size:
                n_updates += apply_batch(conn, list(batch.values()))
                batch.clear()

    if batch:
        n_updates += apply_batch(conn, list(batch.values()))
        batch.clear()

    return {
        "change_files_processed": n_change_files,
        "stop_updates_staged": n_s_nodes,
        "rows_updated_sql": n_updates,
    }
