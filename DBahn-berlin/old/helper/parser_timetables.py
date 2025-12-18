import re
import difflib
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
from config import cfg

from archive_iterator import iter_xml_roots
from xml_extractor import extract_s_id_and_pts

try:
    # schneller für große Batches
    from psycopg2.extras import execute_values
except Exception:
    execute_values = None


def parse_timetables():
    # ----------------------------
    # Einstellungen
    # ----------------------------
    TIMEZONE = "Europe/Berlin"

    MATCH_THRESHOLD = 0.85
    AMBIGUITY_DELTA = 0.02
    BATCH_SIZE = 5000


    def ensure_stops_table(conn):
        create_sql = (cfg.paths.sqls / "CREATE_STOPS.sql").read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

    # ----------------------------
    # Parsing von pt: YYMMDDHHMM -> aware datetime
    # ----------------------------
    _pt_re = re.compile(r"^\d{10}$")  # YYMMDDHHMM

    def parse_pt(pt: str | None, tz: ZoneInfo) -> datetime | None:
        if pt is None:
            return None
        pt = pt.strip()
        if not _pt_re.match(pt):
            return None

        try:
            yy = int(pt[0:2])
            year = 2000 + yy
            month = int(pt[2:4])
            day = int(pt[4:6])
            hour = int(pt[6:8])
            minute = int(pt[8:10])
            return datetime(year, month, day, hour, minute, tzinfo=tz)
        except ValueError:
            return None


    # ----------------------------
    # Normalisierung & Matching
    # ----------------------------
    _umlaut_map = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})
    _punct_re = re.compile(r"[^a-z0-9\s]")
    _ws_re = re.compile(r"\s+")

    _stopwords = {"berlin", "s", "u", "s+u", "u+s"}

    def normalize_name(name: str) -> str:
        name = name.lower().translate(_umlaut_map)

        name = name.replace("straße", "strasse")
        name = name.replace("betriebsbf", "betriebsbahnhof")
        name = name.replace("hauptbahnhof", "hbf")

        name = re.sub(r"\bbahnhof\b", "bf", name)
        name = re.sub(r"\bbhf\b", "bf", name)
        name = re.sub(r"\bbf\.?\b", "bf", name)

        name = name.replace("&", " und ")
        name = _punct_re.sub(" ", name)

        name = re.sub(r"(?<=\w)str\b", "strasse", name)
        name = re.sub(r"\bstr\b", "strasse", name)
        name = re.sub(r"(?<!\s)strasse\b", " strasse", name)

        name = _ws_re.sub(" ", name).strip()
        tokens = [t for t in name.split() if t and t not in _stopwords]
        return " ".join(tokens)


    def token_set(s: str) -> set[str]:
        return {t for t in s.split() if len(t) >= 2}

    def similarity(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        if a == b:
            return 1.0

        if a in b or b in a:
            short, long = (a, b) if len(a) <= len(b) else (b, a)
            substring_score = 0.90 + 0.10 * (len(short) / max(1, len(long)))
        else:
            substring_score = 0.0

        seq_score = difflib.SequenceMatcher(None, a, b).ratio()

        ta, tb = token_set(a), token_set(b)
        token_score = (2 * len(ta & tb) / (len(ta) + len(tb))) if ta and tb else 0.0

        return max(substring_score, seq_score, token_score)


    @dataclass(frozen=True)
    class StationRec:
        eva: int
        raw_name: str
        norm_name: str
        tokens: tuple[str, ...]


    def load_stations(conn) -> list[StationRec]:
        with conn.cursor() as cur:
            cur.execute("SELECT eva, name FROM stationen;")
            rows = cur.fetchall()

        stations: list[StationRec] = []
        for eva, name in rows:
            if not name:
                continue
            norm = normalize_name(name)
            toks = tuple(sorted(token_set(norm)))
            stations.append(StationRec(int(eva), name, norm, toks))
        return stations


    def build_token_index(stations: list[StationRec]) -> dict[str, list[int]]:
        """token -> list of indices into `stations`"""
        idx: dict[str, list[int]] = {}
        for i, st in enumerate(stations):
            for t in st.tokens:
                idx.setdefault(t, []).append(i)
        return idx


    def best_station_match(
        query_name: str,
        stations: list[StationRec],
        token_index: dict[str, list[int]],
        threshold: float,
        ambiguity_delta: float,
    ) -> tuple[int | None, float, str | None, bool]:
        """
        Returns (eva_or_None, best_score, matched_raw_name, is_ambiguous)
        """
        q_norm = normalize_name(query_name)
        if not q_norm:
            return None, 0.0, None, False

        q_tokens = token_set(q_norm)

        cand_indices: set[int] = set()
        for t in q_tokens:
            cand_indices.update(token_index.get(t, []))

        if not cand_indices:
            cand_indices = set(range(len(stations)))

        best_eva: int | None = None
        best_score: float = 0.0
        best_name: str | None = None
        best_norm: str | None = None
        second_best_score: float = 0.0

        for i in cand_indices:
            st = stations[i]
            sc = similarity(q_norm, st.norm_name)

            if sc > best_score:
                second_best_score = best_score
                best_eva, best_score, best_name, best_norm = st.eva, sc, st.raw_name, st.norm_name
            elif sc == best_score and best_norm is not None:
                if len(st.norm_name) < len(best_norm):
                    best_eva, best_score, best_name, best_norm = st.eva, sc, st.raw_name, st.norm_name
            elif sc > second_best_score:
                second_best_score = sc

        if best_eva is None or best_score < threshold:
            return None, best_score, best_name, False

        is_ambiguous = (best_score - second_best_score) < ambiguity_delta
        return best_eva, best_score, best_name, is_ambiguous


    # ----------------------------
    # DB Insert/Upsert Stops
    # ----------------------------
    UPSERT_SQL = """
    INSERT INTO stops (stop_id, eva, ar_ts, dp_ts)
    VALUES %s
    ON CONFLICT (stop_id) DO UPDATE
    SET eva   = EXCLUDED.eva,
        ar_ts = EXCLUDED.ar_ts,
        dp_ts = EXCLUDED.dp_ts;
    """

    def flush_batch(conn, batch: list[tuple], use_execute_values: bool = True) -> int:
        if not batch:
            return 0
        with conn.cursor() as cur:
            if use_execute_values and execute_values is not None:
                execute_values(cur, UPSERT_SQL, batch, page_size=2000)
            else:
                upsert_one = """
                INSERT INTO stops (stop_id, eva, ar_ts, dp_ts)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (stop_id) DO UPDATE
                SET eva = EXCLUDED.eva, ar_ts = EXCLUDED.ar_ts, dp_ts = EXCLUDED.dp_ts;
                """
                cur.executemany(upsert_one, batch)
        conn.commit()
        return len(batch)


    # ----------------------------
    # Main Import
    # ----------------------------
    def import_stops_from_archives(conn, archives_dir: str, pattern: str = "*.tar.gz"):
        ensure_stops_table(conn)

        tz = ZoneInfo(TIMEZONE)

        stations = load_stations(conn)
        token_index = build_token_index(stations)

        unmatched_station_names: set[str] = set()
        ambiguous_station_names: set[str] = set()

        match_cache: dict[str, tuple[int | None, bool]] = {}  # name -> (eva_or_None, ambiguous)

        # dedupe innerhalb eines Batches auf stop_id
        batch: dict[str, tuple] = {}  # stop_id -> row

        total_upserted = 0
        total_seen_stops = 0

        for archive_path, _, root in iter_xml_roots(archives_dir, pattern=pattern):
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
                    station_name, stations, token_index,
                    threshold=MATCH_THRESHOLD,
                    ambiguity_delta=AMBIGUITY_DELTA,
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

                if len(batch) >= BATCH_SIZE:
                    total_upserted += flush_batch(conn, list(batch.values()), use_execute_values=True)
                    batch.clear()

        total_upserted += flush_batch(conn, list(batch.values()), use_execute_values=True)
        batch.clear()

        return {
            "total_seen_stops": total_seen_stops,
            "total_upserted": total_upserted,
            "unmatched_station_names": unmatched_station_names,
            "ambiguous_station_names": ambiguous_station_names,
            "match_cache_size": len(match_cache),
        }
