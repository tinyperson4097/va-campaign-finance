#!/usr/bin/env python3
"""
Seed/refresh the hardcoded section of mappings/name_normalization_mappings.csv
from the frozen rules in functions/name_normalization.py.

Reads the module's constants (EXACT_DOMINION_MATCHES, EXACT_CLEAN_VA_MATCHES)
and derives inline rules by calling normalize_name() itself -- this script
never modifies that file, and rerunning it keeps the hardcoded rows in exact
sync if new additive rules land there.

Rows with source='hardcoded' are owned by this script: rerunning replaces
them. Rows with any other source (e.g. 'agent') are preserved untouched.
"""

import csv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from functions.name_normalization import (
    EXACT_CLEAN_VA_MATCHES,
    EXACT_DOMINION_MATCHES,
    normalize_name,
)

CSV_PATH = Path(__file__).parent / "name_normalization_mappings.csv"
FIELDS = ["entity_type", "raw_name", "normalized_name", "source", "notes"]

# Inline hardcoded rules in normalize_name() that aren't module constants:
# derived by calling the function, so this stays correct even if the frozen
# file gains additive fixes.
INLINE_CANDIDATE_RULES = ["CAT PORTERFIELD"]


def build_hardcoded_rows():
    rows = []
    for raw in sorted(EXACT_DOMINION_MATCHES):
        rows.append({
            "entity_type": "entity",
            "raw_name": raw,
            "normalized_name": "DOMINION ENERGY",
            "source": "hardcoded",
            "notes": "EXACT_DOMINION_MATCHES in functions/name_normalization.py",
        })
    for raw in sorted(EXACT_CLEAN_VA_MATCHES):
        rows.append({
            "entity_type": "entity",
            "raw_name": raw,
            "normalized_name": "CLEAN VA FUND",
            "source": "hardcoded",
            "notes": "EXACT_CLEAN_VA_MATCHES in functions/name_normalization.py",
        })
    for raw in INLINE_CANDIDATE_RULES:
        rows.append({
            "entity_type": "candidate",
            "raw_name": raw,
            "normalized_name": normalize_name(raw, is_individual=True),
            "source": "hardcoded",
            "notes": "inline rule in normalize_name()",
        })
    return rows


def main():
    preserved = []
    if CSV_PATH.exists():
        with open(CSV_PATH, newline="") as f:
            preserved = [r for r in csv.DictReader(f) if r["source"] != "hardcoded"]

    hardcoded = build_hardcoded_rows()
    hardcoded_raw_names = {(r["entity_type"], r["raw_name"].strip().upper()) for r in hardcoded}

    # Hardcoded always wins: drop any non-hardcoded row that collides.
    kept = [
        r for r in preserved
        if (r["entity_type"], r["raw_name"].strip().upper()) not in hardcoded_raw_names
    ]
    dropped = len(preserved) - len(kept)

    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(hardcoded + kept)

    print(f"Wrote {len(hardcoded)} hardcoded rows, preserved {len(kept)} other rows"
          + (f", dropped {dropped} colliding non-hardcoded rows" if dropped else ""))


if __name__ == "__main__":
    main()
