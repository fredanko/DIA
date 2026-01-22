from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, Tuple

from .io_archives import iter_xml_roots


def count_stop_id_duplicates(
    archives_dir: str | Path,
    *,
    pattern: str = "*.tar.gz",
) -> dict:
    archives_dir = Path(archives_dir)

    counts: Dict[str, int] = {}
    files = 0
    total = 0

    for _, _, root in iter_xml_roots(archives_dir, pattern=pattern):
        files += 1

        for s in root.findall("s"):
            stop_id = s.get("id")
            if not stop_id:
                continue
            total += 1
            counts[stop_id] = counts.get(stop_id, 0) + 1

    unique = len(counts)
    duplicates = total - unique
    duplicate_ids = sum(1 for c in counts.values() if c > 1)

    return {
        "files_processed": files,
        "stop_id_total": total,
        "stop_id_unique": unique,
        "stop_id_duplicates": duplicates,
        "duplicate_ids": duplicate_ids,
    }