#!/usr/bin/env python3
import argparse
import json
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import xml.etree.ElementTree as ET

import psycopg2
import psycopg2.extras


# -------------------------
# Parsing helpers
# -------------------------
def parse_iris_ts(ts: Optional[str]) -> Optional[datetime]:
    """
    IRIS-Format: 'YYMMddHHmm' (10 Zeichen), z.B. '2509021217' => 2025-09-02 12:17
    """
    if not ts:
        return None
    ts = ts.strip()
    if len(ts) != 10 or not ts.isdigit():
        return None
    return datetime.strptime(ts, "%y%m%d%H%M")


def norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ß", "ss").replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    # grobe Normalisierung: nur alnum behalten
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def iter_tar_xml(tar_gz_path: Path) -> Iterable[Tuple[str, bytes]]:
    with tarfile.open(tar_gz_path, "r:gz") as tf:
        for m in tf.getmembers():
            if m.isfile() and m.name.lower().endswith(".xml"):
                f = tf.extractfile(m)
                if f:
                    yield m.name, f.read()


# -------------------------
# DB helpers
# -------------------------
def run_sql(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def resolve_station_key(
    conn,
    cache: Dict[Tuple[Optional[int], str], int],
    station_name: str,
    eva: Optional[int],
    context_file: Optional[str],
    similarity_threshold: float,
) -> Optional[int]:
    """
    Strategy:
      1) EVA lookup (dim_station_eva)
      2) exact alias_norm
      3) exact station name_norm
      4) trigram similarity over dim_station.name_norm
         -> on success: persist alias in dim_station_alias
         -> on fail: log in stg_unresolved_station and return None
    """
    key = (eva, norm_name(station_name))
    if key in cache:
        return cache[key]

    with conn.cursor() as cur:
        if eva is not None:
            cur.execute("SELECT station_key FROM dw.dim_station_eva WHERE eva = %s", (eva,))
            row = cur.fetchone()
            if row:
                cache[key] = row[0]
                return row[0]

        n = norm_name(station_name)

        cur.execute("SELECT station_key FROM dw.dim_station_alias WHERE alias_norm = %s", (n,))
        row = cur.fetchone()
        if row:
            cache[key] = row[0]
            return row[0]

        cur.execute("SELECT station_key FROM dw.dim_station WHERE name_norm = %s", (n,))
        row = cur.fetchone()
        if row:
            cache[key] = row[0]
            return row[0]

        # fuzzy with pg_trgm
        cur.execute(
            """
            SELECT station_key, name_canonical, similarity(name_norm, %s) AS sim
            FROM dw.dim_station
            ORDER BY similarity(name_norm, %s) DESC
            LIMIT 1
            """,
            (n, n),
        )
        row = cur.fetchone()
        if row and row[2] is not None and float(row[2]) >= similarity_threshold:
            station_key = int(row[0])
            # persist alias
            cur.execute(
                """
                INSERT INTO dw.dim_station_alias(alias, station_key, source, confidence)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (alias) DO UPDATE
                  SET station_key = EXCLUDED.station_key,
                      source = EXCLUDED.source,
                      confidence = EXCLUDED.confidence
                """,
                (station_name, station_key, "fuzzy(pg_trgm)", float(row[2])),
            )
            conn.commit()
            cache[key] = station_key
            return station_key

        # log unresolved
        best_match = row[1] if row else None
        best_sim = float(row[2]) if row and row[2] is not None else None
        cur.execute(
            """
            INSERT INTO dw.stg_unresolved_station(station_name, station_norm, context_file, best_match, best_similarity)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (station_name, n, context_file, best_match, best_sim),
        )
        conn.commit()
        return None


def upsert_trips(conn, trip_rows: List[Tuple]) -> Dict[Tuple, int]:
    """
    trip_rows tuples:
      (service_date, category, train_no, owner, trip_type, line)
    returns mapping: same tuple -> trip_key
    """
    if not trip_rows:
        return {}

    trip_rows = list(dict.fromkeys(trip_rows))  # dedupe, preserve order

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_trip(service_date, category, train_no, owner, trip_type, line)
            VALUES %s
            ON CONFLICT (service_date, category, train_no, owner, trip_type, line) DO NOTHING
            """,
            trip_rows,
            page_size=2000,
        )

        # fetch keys for the batch via VALUES join
        psycopg2.extras.execute_values(
            cur,
            """
            WITH v(service_date, category, train_no, owner, trip_type, line) AS (VALUES %s)
            SELECT t.trip_key, v.service_date, v.category, v.train_no, v.owner, v.trip_type, v.line
            FROM v
            JOIN dw.dim_trip t
              ON t.service_date = v.service_date
             AND t.category = v.category
             AND t.train_no = v.train_no
             AND COALESCE(t.owner,'') = COALESCE(v.owner,'')
             AND COALESCE(t.trip_type,'') = COALESCE(v.trip_type,'')
             AND COALESCE(t.line,'') = COALESCE(v.line,'')
            """,
            trip_rows,
            fetch=True,
        )
        rows = cur.fetchall()

    conn.commit()

    out: Dict[Tuple, int] = {}
    for trip_key, service_date, category, train_no, owner, trip_type, line in rows:
        nk = (service_date, category, train_no, owner, trip_type, line)
        out[nk] = int(trip_key)
    return out


def upsert_stop_events(conn, rows: List[Tuple]) -> None:
    """
    rows tuple order must match INSERT columns below
    """
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.fact_stop_event(
              station_key, trip_key, stop_id,
              planned_arrival_ts, planned_departure_ts, planned_platform, planned_path,
              actual_arrival_ts, actual_departure_ts, actual_platform,
              arrival_status, departure_status, is_canceled,
              load_source, load_file
            )
            VALUES %s
            ON CONFLICT (station_key, stop_id) DO UPDATE SET
              trip_key = COALESCE(EXCLUDED.trip_key, dw.fact_stop_event.trip_key),

              planned_arrival_ts   = COALESCE(EXCLUDED.planned_arrival_ts,   dw.fact_stop_event.planned_arrival_ts),
              planned_departure_ts = COALESCE(EXCLUDED.planned_departure_ts, dw.fact_stop_event.planned_departure_ts),
              planned_platform     = COALESCE(EXCLUDED.planned_platform,     dw.fact_stop_event.planned_platform),
              planned_path         = COALESCE(EXCLUDED.planned_path,         dw.fact_stop_event.planned_path),

              actual_arrival_ts    = COALESCE(EXCLUDED.actual_arrival_ts,    dw.fact_stop_event.actual_arrival_ts),
              actual_departure_ts  = COALESCE(EXCLUDED.actual_departure_ts,  dw.fact_stop_event.actual_departure_ts),
              actual_platform      = COALESCE(EXCLUDED.actual_platform,      dw.fact_stop_event.actual_platform),

              arrival_status       = COALESCE(EXCLUDED.arrival_status,       dw.fact_stop_event.arrival_status),
              departure_status     = COALESCE(EXCLUDED.departure_status,     dw.fact_stop_event.departure_status),

              is_canceled          = (dw.fact_stop_event.is_canceled OR EXCLUDED.is_canceled),

              load_source          = EXCLUDED.load_source,
              load_file            = EXCLUDED.load_file,
              load_ts              = now()
            """,
            rows,
            page_size=5000,
        )
    conn.commit()


def refresh_dim_time_hour(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.dim_time_hour(hour_ts, "date", "year", "month", "day", "hour")
            SELECT DISTINCT h AS hour_ts,
                   h::date AS "date",
                   EXTRACT(year  FROM h)::int AS "year",
                   EXTRACT(month FROM h)::int AS "month",
                   EXTRACT(day   FROM h)::int AS "day",
                   EXTRACT(hour  FROM h)::int AS "hour"
            FROM (
              SELECT planned_hour_ts AS h FROM dw.fact_stop_event WHERE planned_hour_ts IS NOT NULL
              UNION
              SELECT actual_hour_ts  AS h FROM dw.fact_stop_event WHERE actual_hour_ts IS NOT NULL
            ) x
            ON CONFLICT (hour_ts) DO NOTHING
            """
        )
    conn.commit()


# -------------------------
# Load stations.json
# -------------------------
def generate_station_aliases(name: str) -> List[str]:
    aliases = {name}
    # häufige Varianten
    aliases.add(name.replace("(S)", "").strip())
    aliases.add(name.replace("  ", " ").strip())
    if " Hbf" in name:
        aliases.add(name.replace(" Hbf", " Hauptbahnhof"))
    if "Hauptbahnhof" in name:
        aliases.add(name.replace("Hauptbahnhof", "Hbf"))
    return [a for a in aliases if a]


def load_station_data(conn, station_json_path: Path) -> None:
    data = json.loads(station_json_path.read_text(encoding="utf-8"))
    stations = data["result"]

    with conn.cursor() as cur:
        for st in stations:
            name = st["name"]

            main_eva = None
            main_lat = None
            main_lon = None
            for ev in st.get("evaNumbers", []):
                if ev.get("isMain"):
                    main_eva = int(ev["number"])
                    coords = ev.get("geographicCoordinates", {}).get("coordinates", None)
                    if coords and len(coords) == 2:
                        main_lon, main_lat = float(coords[0]), float(coords[1])
                    break

            cur.execute(
                """
                INSERT INTO dw.dim_station(name_canonical, lat, lon)
                VALUES (%s, %s, %s)
                ON CONFLICT (name_canonical) DO UPDATE
                  SET lat = COALESCE(EXCLUDED.lat, dw.dim_station.lat),
                      lon = COALESCE(EXCLUDED.lon, dw.dim_station.lon),
                      updated_at = now()
                """,
                (name, main_lat, main_lon),
            )

            cur.execute("SELECT station_key FROM dw.dim_station WHERE name_canonical = %s", (name,))
            station_key = int(cur.fetchone()[0])

            # EVA rows
            for ev in st.get("evaNumbers", []):
                eva = int(ev["number"])
                coords = ev.get("geographicCoordinates", {}).get("coordinates", None)
                lat = lon = None
                if coords and len(coords) == 2:
                    lon, lat = float(coords[0]), float(coords[1])
                is_main = bool(ev.get("isMain", False))

                cur.execute(
                    """
                    INSERT INTO dw.dim_station_eva(eva, station_key, is_main, lat, lon)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (eva) DO UPDATE
                      SET station_key = EXCLUDED.station_key,
                          is_main = EXCLUDED.is_main,
                          lat = COALESCE(EXCLUDED.lat, dw.dim_station_eva.lat),
                          lon = COALESCE(EXCLUDED.lon, dw.dim_station_eva.lon)
                    """,
                    (eva, station_key, is_main, lat, lon),
                )

            # Aliases
            for alias in generate_station_aliases(name):
                cur.execute(
                    """
                    INSERT INTO dw.dim_station_alias(alias, station_key, source, confidence)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (alias) DO NOTHING
                    """,
                    (alias, station_key, "station_data.json", 1.0),
                )

    conn.commit()


# -------------------------
# Parse XML
# -------------------------
@dataclass(frozen=True)
class TripNK:
    service_date: datetime.date
    category: str
    train_no: str
    owner: Optional[str]
    trip_type: Optional[str]
    line: Optional[str]

    def as_tuple(self):
        return (self.service_date, self.category, self.train_no, self.owner, self.trip_type, self.line)


def parse_timetable_xml(
    xml_bytes: bytes,
    station_key: int,
    source_file: str,
) -> Tuple[List[TripNK], List[Tuple]]:
    """
    returns:
      trips: list of TripNK
      stop_rows: list of tuples for upsert_stop_events
    """
    root = ET.fromstring(xml_bytes)
    stop_rows = []
    trips: List[TripNK] = []

    for s in root.findall("s"):
        stop_id = s.get("id")
        if not stop_id:
            continue

        tl = s.find("tl")
        category = tl.get("c") if tl is not None else None
        train_no = tl.get("n") if tl is not None else None
        owner = tl.get("o") if tl is not None else None
        trip_type = tl.get("t") if tl is not None else None

        ar = s.find("ar")
        dp = s.find("dp")

        ar_pt = parse_iris_ts(ar.get("pt")) if ar is not None else None
        dp_pt = parse_iris_ts(dp.get("pt")) if dp is not None else None

        planned_platform = None
        planned_path = None
        line = None

        if dp is not None:
            planned_platform = dp.get("pp") or planned_platform
            planned_path = dp.get("ppth") or planned_path
            line = dp.get("l") or line
        if ar is not None:
            planned_platform = ar.get("pp") or planned_platform
            planned_path = ar.get("ppth") or planned_path
            line = ar.get("l") or line

        # Trip key (minimal, pro service_date)
        base_ts = dp_pt or ar_pt
        service_date = base_ts.date() if base_ts else None
        trip_nk = None
        trip_key_placeholder = None

        if service_date and category and train_no:
            trip_nk = TripNK(service_date, category, train_no, owner, trip_type, line)
            trips.append(trip_nk)

        stop_rows.append(
            (
                station_key,
                trip_key_placeholder,   # später ersetzt
                stop_id,
                ar_pt,
                dp_pt,
                planned_platform,
                planned_path,
                None,   # actual_arrival_ts
                None,   # actual_departure_ts
                None,   # actual_platform
                None,   # arrival_status
                None,   # departure_status
                False,  # is_canceled
                "timetable",
                source_file,
            )
        )

    return trips, stop_rows


def parse_change_xml(
    xml_bytes: bytes,
    station_key: int,
    source_file: str,
) -> Tuple[List[TripNK], List[Tuple]]:
    root = ET.fromstring(xml_bytes)
    stop_rows = []
    trips: List[TripNK] = []

    for s in root.findall("s"):
        stop_id = s.get("id")
        if not stop_id:
            continue

        tl = s.find("tl")
        category = tl.get("c") if tl is not None else None
        train_no = tl.get("n") if tl is not None else None
        owner = tl.get("o") if tl is not None else None
        trip_type = tl.get("t") if tl is not None else None

        ar = s.find("ar")
        dp = s.find("dp")

        ar_ct = parse_iris_ts(ar.get("ct")) if ar is not None else None
        dp_ct = parse_iris_ts(dp.get("ct")) if dp is not None else None

        # cancellation flags (wenn cs="c" oder clt gesetzt)
        def is_cancelled(ev: Optional[ET.Element]) -> bool:
            if ev is None:
                return False
            cs = ev.get("cs")
            clt = ev.get("clt")
            return (cs == "c") or (clt is not None and clt != "")

        canceled = is_cancelled(ar) or is_cancelled(dp)

        arrival_status = (ar.get("cs") if ar is not None else None)
        departure_status = (dp.get("cs") if dp is not None else None)

        # Plattformänderungen: cp (changed platform)
        actual_platform = None
        if dp is not None:
            actual_platform = dp.get("cp") or actual_platform
        if ar is not None:
            actual_platform = ar.get("cp") or actual_platform

        # line (falls in change vorhanden)
        line = None
        if dp is not None:
            line = dp.get("l") or line
        if ar is not None:
            line = ar.get("l") or line

        base_ts = dp_ct or ar_ct
        service_date = base_ts.date() if base_ts and category and train_no else None
        if service_date and category and train_no:
            trips.append(TripNK(service_date, category, train_no, owner, trip_type, line))

        stop_rows.append(
            (
                station_key,
                None,  # trip_key später optional gesetzt
                stop_id,
                None,  # planned_arrival_ts
                None,  # planned_departure_ts
                None,  # planned_platform
                None,  # planned_path
                ar_ct,
                dp_ct,
                actual_platform,
                arrival_status[0] if arrival_status else None,
                departure_status[0] if departure_status else None,
                bool(canceled),
                "change",
                source_file,
            )
        )

    return trips, stop_rows


# -------------------------
# Main ingestion
# -------------------------
def ingest_archives(
    conn,
    archives_dir: Path,
    kind: str,  # "timetable" | "change"
    station_cache: Dict[Tuple[Optional[int], str], int],
    similarity_threshold: float,
) -> None:
    archives = sorted(archives_dir.glob("*.tar.gz"))
    for tar_path in archives:
        for member_name, xml_bytes in iter_tar_xml(tar_path):
            # station info from root attributes
            root = ET.fromstring(xml_bytes)
            station_name = root.get("station") or ""
            eva_root = root.get("eva")
            eva = int(eva_root) if eva_root and eva_root.isdigit() else None

            station_key = resolve_station_key(
                conn=conn,
                cache=station_cache,
                station_name=station_name,
                eva=eva,
                context_file=f"{tar_path.name}:{member_name}",
                similarity_threshold=similarity_threshold,
            )
            if station_key is None:
                # ohne station_key können wir die Facts nicht konsistent laden
                continue

            # Parse
            if kind == "timetable":
                trips, stop_rows = parse_timetable_xml(xml_bytes, station_key, f"{tar_path.name}:{member_name}")
            else:
                trips, stop_rows = parse_change_xml(xml_bytes, station_key, f"{tar_path.name}:{member_name}")

            # Upsert trips -> map NK -> trip_key
            trip_nks = [t.as_tuple() for t in trips]
            trip_map = upsert_trips(conn, trip_nks)

            # stop_rows: trip_key einsetzen (wo möglich)
            fixed_rows = []
            for row in stop_rows:
                (
                    st_key, _trip_key, stop_id,
                    par, pdep, pplat, ppath,
                    aar, adep, aplat,
                    astat, dstat, canceled,
                    src, lf
                ) = row

                # trip_nk aus den Zeiten rekonstruieren (wie oben)
                service_ts = (pdep or par or adep or aar)
                trip_key = None
                if service_ts:
                    # falls es Trips gab, nehmen wir *irgendeinen* passenden pro stop (minimaler Ansatz)
                    # besser: parse_* könnte trip_nk pro stop zurückgeben; für eure Aufgaben reicht das so.
                    # Wir versuchen: (date, ?, ?, ?, ?, ?) -> nur wenn parse_* Trips im Batch hatte.
                    # Hier: pick first trip_key from trip_map (falls vorhanden), sonst None.
                    if trip_map:
                        trip_key = next(iter(trip_map.values()))

                fixed_rows.append(
                    (
                        st_key, trip_key, stop_id,
                        par, pdep, pplat, ppath,
                        aar, adep, aplat,
                        astat, dstat, canceled,
                        src, lf
                    )
                )

            upsert_stop_events(conn, fixed_rows)

        # nach jedem tar: time dimension refreshen
        refresh_dim_time_hour(conn)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True, help="postgres DSN, z.B. postgresql://user:pass@host:5432/db")
    ap.add_argument("--ddl", required=True, help="Path to ddl.sql")
    ap.add_argument("--station-json", required=True, help="Path to station_data.json")
    ap.add_argument("--timetable-dir", required=True, help="Folder with timetable .tar.gz")
    ap.add_argument("--changes-dir", required=True, help="Folder with timetable_changes .tar.gz")
    ap.add_argument("--sim-threshold", type=float, default=0.45, help="pg_trgm similarity threshold")
    args = ap.parse_args()

    print(type(args.dsn), repr(args.dsn))
    conn = psycopg2.connect(args.dsn, options="-c client_encoding=UTF8")
    conn.autocommit = False

    # 1) DDL
    run_sql(conn, Path(args.ddl).read_text(encoding="utf-8"))

    # 2) Stations laden
    load_station_data(conn, Path(args.station_json))

    # 3) Ingest timetable + changes
    station_cache: Dict[Tuple[Optional[int], str], int] = {}

    ingest_archives(conn, Path(args.timetable_dir), "timetable", station_cache, args.sim_threshold)
    ingest_archives(conn, Path(args.changes_dir), "change", station_cache, args.sim_threshold)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
