from __future__ import annotations

import re
import difflib
from dataclasses import dataclass

_umlaut_map = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})
_punct_re = re.compile(r"[^a-z0-9\s]")
_ws_re = re.compile(r"\s+")

_stopwords = {"berlin", "s", "u", "s+u", "u+s"}

def normalize_name(name: str) -> str:
    # Translate German Umlaute
    name = name.lower().translate(_umlaut_map)

    # standardize naming convetions of train stations
    name = name.replace("straße", "strasse")
    name = name.replace("betriebsbf", "betriebsbahnhof")
    name = name.replace("hauptbahnhof", "hbf")

    # standardize naming conventions of "bahnhof"
    name = re.sub(r"\bbahnhof\b", "bf", name)
    name = re.sub(r"\bbhf\b", "bf", name)
    name = re.sub(r"\bbf\.?\b", "bf", name)

    # standardize further
    name = name.replace("&", " und ")
    name = _punct_re.sub(" ", name)

    # standardize term "strasse" and combination of "strasse" and whitespaces
    name = re.sub(r"(?<=\w)str\b", "strasse", name)
    name = re.sub(r"\bstr\b", "strasse", name)
    name = re.sub(r"(?<!\s)strasse\b", " strasse", name)

    name = _ws_re.sub(" ", name).strip()

    # Remove stop words
    tokens = [t for t in name.split() if t and t not in _stopwords]
    return " ".join(tokens)

def token_set(s: str) -> set[str]:
    return {t for t in s.split() if len(t) >= 2}

def similarity(a: str, b: str) -> float:
    # hardcoded metrics
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    seq = difflib.SequenceMatcher(None, a, b).ratio()

    ta, tb = token_set(a), token_set(b)
    if not ta or not tb:
        return seq

    # similar idea to jaccard similarity but does not use union but max length
    overlap = len(ta & tb) / max(len(ta), len(tb))
    return max(seq, overlap)

@dataclass(frozen=True, slots=True)
class StationRec:
    eva: int
    name: str
    norm: str
    toks: tuple[str, ...]

def build_token_index(stations: list[StationRec]) -> dict[str, list[int]]:
    idx: dict[str, list[int]] = {}
    for i, st in enumerate(stations):
        for t in st.toks:
            idx.setdefault(t, []).append(i)
    return idx


def best_station_match(
    query_name: str,
    stations: list[StationRec],
    token_index: dict[str, list[int]],
    threshold: float,
    ambiguity_delta: float,
) -> tuple[int | None, float, str | None, bool]:
    
    q_norm = normalize_name(query_name)
    if not q_norm:
        return None, 0.0, None, False

    q_tokens = token_set(q_norm)

    # Consider Candidates with same tokens, and if no tokens are same, consider all
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

    # Find candidate with the best similarity
    for i in cand_indices:
        st = stations[i]
        sc = similarity(q_norm, st.norm)

        if sc > best_score:
            second_best_score = best_score
            best_eva, best_score, best_name, best_norm = st.eva, sc, st.name, st.norm
        elif sc == best_score and best_norm is not None:
            if len(st.norm) < len(best_norm):
                best_eva, best_score, best_name, best_norm = st.eva, sc, st.name, st.norm
        elif sc > second_best_score:
            second_best_score = sc

    if best_eva is None or best_score < threshold:
        return None, best_score, best_name, False

    is_ambiguous = (best_score - second_best_score) < ambiguity_delta
    return best_eva, best_score, best_name, is_ambiguous
