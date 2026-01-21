from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover
    execute_values = None

from .sql import load_sql

TrainKey = Tuple[str, str, str, Optional[str], Optional[str], Optional[str]]  # owner, category, number, trip_type, filter_flags, line


def ensure_dimensions(conn) -> None:
    """Create dimension tables if they do not exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_dim_time.sql"))
        cur.execute(load_sql("schema/create_dim_train.sql"))
    conn.commit()


def get_time_ids(conn, dts: Iterable[datetime]) -> Dict[datetime, int]:
    """Ensure all timestamps exist in dim_time and return mapping dt -> time_id."""
    dts_list = [dt for dt in dts if dt is not None]
    if not dts_list:
        return {}

    if execute_values is None:  # pragma: no cover
        raise RuntimeError("psycopg2.extras.execute_values not available")

    uniq = sorted(set(dts_list))
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO dim_time(ts) VALUES %s ON CONFLICT (ts) DO NOTHING",
            [(dt,) for dt in uniq],
            page_size=1000,
        )
        cur.execute("SELECT time_id, ts FROM dim_time WHERE ts = ANY(%s)", (uniq,))
        rows = cur.fetchall()
    conn.commit()
    return {ts: time_id for (time_id, ts) in rows}


def get_train_ids(conn, trains: Iterable[TrainKey]) -> Dict[TrainKey, int]:
    """Ensure all train keys exist in dim_train and return mapping key -> train_id."""
    trains_list = [t for t in trains if t is not None]
    if not trains_list:
        return {}

    if execute_values is None:  # pragma: no cover
        raise RuntimeError("psycopg2.extras.execute_values not available")

    seen: set[TrainKey] = set()
    uniq: list[TrainKey] = []
    for k in trains_list:
        if k not in seen:
            seen.add(k)
            uniq.append(k)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO dim_train(owner, category, number, trip_type, filter_flags, line)
               VALUES %s
               ON CONFLICT (owner, category, number, trip_type, filter_flags, line) DO NOTHING""",
            uniq,
            page_size=1000,
        )

        # Build a VALUES clause safely via mogrify
        values_sql = b",".join(
            cur.mogrify("(%s,%s,%s,%s,%s,%s)", t) for t in uniq
        ).decode("utf-8")

        cur.execute(
            f"""WITH v(owner, category, number, trip_type, filter_flags, line) AS (VALUES {values_sql})
                SELECT d.train_id, v.owner, v.category, v.number, v.trip_type, v.filter_flags, v.line
                FROM v
                JOIN dim_train d
                  ON d.owner = v.owner
                 AND d.category = v.category
                 AND d.number = v.number
                 AND (d.trip_type IS NOT DISTINCT FROM v.trip_type)
                 AND (d.filter_flags IS NOT DISTINCT FROM v.filter_flags)
                 AND (d.line IS NOT DISTINCT FROM v.line)"""
        )
        rows = cur.fetchall()
    conn.commit()

    return {(o, c, n, t, f, l): train_id for (train_id, o, c, n, t, f, l) in rows}
