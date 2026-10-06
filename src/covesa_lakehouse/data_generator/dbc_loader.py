"""Load community-maintained DBC files that are not strictly spec-compliant.

opendbc files are reverse-engineered and some break the DBC grammar that cantools enforces.
Rather than dropping a platform, we repair the two defects seen in practice:

* signal names starting with a digit (``SG_ 0_COUNTER``) -> prefixed with ``_``
* 29-bit frame ids written without the extended-frame flag (bit 31) -> flag added
"""

from __future__ import annotations

import re
from pathlib import Path

import cantools
from cantools.database.can import Database

_EXTENDED_FLAG = 0x80000000
_MAX_STANDARD_ID = 0x7FF

_DIGIT_SIGNAL = re.compile(r"^(\s*SG_\s+)(\d\w*)", re.MULTILINE)
# Statements that reference a signal by name right after "<frame id> ".
_DIGIT_SIGNAL_REF = re.compile(r"^((?:CM_ SG_|VAL_|BA_ \"\w+\" SG_)\s+\d+\s+)(\d\w*)", re.MULTILINE)
_MESSAGE_ID = re.compile(r"^BO_\s+(\d+)\s", re.MULTILINE)
_ID_REF = re.compile(
    r"^((?:BO_|CM_ BO_|CM_ SG_|VAL_|BA_ \"\w+\" (?:BO_|SG_)|BO_TX_BU_)\s+)(\d+)", re.MULTILINE
)


def sanitize_dbc(text: str) -> tuple[str, list[str]]:
    """Return repaired DBC text and a human-readable list of the fixes applied."""
    fixes: list[str] = []

    renamed = sorted(set(_DIGIT_SIGNAL.findall(text)), key=lambda m: m[1])
    if renamed:
        text = _DIGIT_SIGNAL.sub(r"\1_\2", text)
        text = _DIGIT_SIGNAL_REF.sub(r"\1_\2", text)
        fixes.append(f"prefixed {len(renamed)} signal name(s) starting with a digit")

    bad_ids = {
        int(i) for i in _MESSAGE_ID.findall(text) if _MAX_STANDARD_ID < int(i) < _EXTENDED_FLAG
    }
    if bad_ids:

        def flag(m: re.Match[str]) -> str:
            frame_id = int(m.group(2))
            return m.group(1) + str(frame_id | _EXTENDED_FLAG if frame_id in bad_ids else frame_id)

        text = _ID_REF.sub(flag, text)
        fixes.append(f"marked {len(bad_ids)} 29-bit frame id(s) as extended")

    return text, fixes


def load_dbc(path: Path) -> tuple[Database, list[str]]:
    text, fixes = sanitize_dbc(path.read_text(encoding="utf-8", errors="replace"))
    db = cantools.database.load_string(text, database_format="dbc", strict=False)
    assert isinstance(db, Database)
    return db, fixes
