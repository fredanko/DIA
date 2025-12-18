from __future__ import annotations

from pathlib import Path
import tarfile
import xml.etree.ElementTree as ET
from typing import Iterator

def iter_xml_roots(archives_dir, pattern= "*.tar.gz"):
    archives_dir = Path(archives_dir)

    for archive_path in sorted(archives_dir.glob(pattern)):
        try:
            with tarfile.open(archive_path, mode="r:gz") as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    if not member.name.lower().endswith(".xml"):
                        continue

                    extracted = tar.extractfile(member)
                    if extracted is None:
                        continue

                    try:
                        with extracted as f:
                            tree = ET.parse(f)
                            yield archive_path, member.name, tree.getroot()
                    except ET.ParseError as e:
                        print(f"[WARN] XML ParseError in {archive_path}::{member.name}: {e}")

        except (tarfile.ReadError, OSError) as e:
            print(f"[WARN] Could not read: {archive_path} ({e})")