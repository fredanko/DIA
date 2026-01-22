from __future__ import annotations

from pathlib import Path
import tarfile
import xml.etree.ElementTree as ET
from typing import Iterator

# This is a helper function (iterator) that is designed to yield the xml roots from the source folders
def iter_xml_roots(archives_dir: str | Path, pattern: str = "*.tar.gz"
                   ) -> Iterator[tuple[Path, str, ET.Element]]:
    archives_dir = Path(archives_dir)
    # Iterate throgh every .tar.gz
    for archive_path in sorted(archives_dir.glob(pattern)):
        try:
            with tarfile.open(archive_path, mode="r:gz") as tar:
                # iterate trough every .xml in the tar files
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
                            # yield root
                            yield archive_path, member.name, tree.getroot()
                    except ET.ParseError as e:
                        print(f"[WARN] XML ParseError in {archive_path}::{member.name}: {e}")

        except (tarfile.ReadError, OSError) as e:
            print(f"[WARN] Could not read archive: {archive_path} ({e})")
