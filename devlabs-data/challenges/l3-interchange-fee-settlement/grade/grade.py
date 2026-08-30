#!/usr/bin/env python3
"""Grade Interchange Fee Settlement Summary.

Compares candidate CSV output against the reference CSV.

Usage:
  python grade.py --candidate <file-or-dir> --reference <file-or-dir>

Prints a JSON object to stdout:
  { "passed": bool, "summary": str, "checks": [{ "id", "label", "passed", "detail"? }] }

Exit 0 when the grader itself ran (pass or fail). Exit 2 on usage / I/O errors.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

KEYS = ("country_code", "entry_mode")
INT_COLS = ("txn_count", "distinct_rate_versions_used")
FEE_COL = "total_fee"
FEE_ATOL = 0.01
REQUIRED = KEYS + INT_COLS + (FEE_COL,)


def check(cid: str, label: str, passed: bool, detail: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"id": cid, "label": label, "passed": passed}
    if detail:
        out["detail"] = detail
    return out


def find_csv_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    files = [
        p
        for p in path.rglob("*")
        if p.is_file()
        and p.suffix.lower() == ".csv"
        and not p.name.startswith(".")
        and not p.name.endswith(".crc")
        and "_temporary" not in p.parts
        and ".spark-staging" not in p.parts
    ]
    return sorted(files)


def row_key(row: dict[str, str]) -> str:
    return "\u0001".join(row[k].strip() for k in KEYS)


def load_csv_map(path: Path) -> tuple[dict[str, dict[str, str]], str | None]:
    files = find_csv_files(path)
    if not files:
        return {}, f"no CSV files under {path}"
    rows: dict[str, dict[str, str]] = {}
    for f in files:
        with f.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                return {}, f"{f.name}: missing header"
            missing = [c for c in REQUIRED if c not in reader.fieldnames]
            if missing:
                return {}, f"{f.name}: missing columns {missing}; have {list(reader.fieldnames)}"
            for i, raw in enumerate(reader, start=2):
                row = {c: (raw.get(c) or "").strip() for c in REQUIRED}
                if any(row[k] == "" for k in KEYS):
                    return {}, f"{f.name}:{i} missing grade key {KEYS}"
                k = row_key(row)
                if k in rows:
                    return {}, f"{f.name}:{i} duplicate key {tuple(row[c] for c in KEYS)}"
                rows[k] = row
    return rows, None


def parse_int(value: str, col: str) -> tuple[int | None, str | None]:
    try:
        return int(float(value)), None
    except (TypeError, ValueError):
        return None, f"invalid int for {col}: {value!r}"


def parse_fee(value: str) -> tuple[float | None, str | None]:
    try:
        return float(value), None
    except (TypeError, ValueError):
        return None, f"invalid fee: {value!r}"


def compare(candidate: dict[str, dict[str, str]], reference: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    missing = [k for k in reference if k not in candidate]
    extra = [k for k in candidate if k not in reference]
    checks.append(
        check(
            "keys_match",
            "Row keys (country_code, entry_mode) match",
            not missing and not extra,
            f"missing={len(missing)} extra={len(extra)}",
        )
    )
    if missing or extra:
        return checks

    int_mismatch = 0
    fee_mismatch = 0
    examples: list[str] = []
    for k, exp in reference.items():
        got = candidate[k]
        key_label = ",".join(exp[c] for c in KEYS)
        for col in INT_COLS:
            ev, eerr = parse_int(exp[col], col)
            cv, cerr = parse_int(got[col], col)
            if eerr or cerr or ev != cv:
                int_mismatch += 1
                if len(examples) < 5:
                    examples.append(f"{key_label}:{col} expected={exp[col]} got={got[col]}")
        ev, eerr = parse_fee(exp[FEE_COL])
        cv, cerr = parse_fee(got[FEE_COL])
        if eerr or cerr or ev is None or cv is None or abs(ev - cv) > FEE_ATOL:
            fee_mismatch += 1
            if len(examples) < 5:
                examples.append(f"{key_label}:{FEE_COL} expected={exp[FEE_COL]} got={got[FEE_COL]}")

    detail = f"int_mismatch={int_mismatch} fee_mismatch={fee_mismatch}"
    if examples:
        detail += f"; e.g. {'; '.join(examples)}"
    checks.append(
        check(
            "values_match",
            "txn_count, total_fee, distinct_rate_versions_used match",
            int_mismatch == 0 and fee_mismatch == 0,
            detail,
        )
    )
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Grade interchange settlement CSV")
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    args = parser.parse_args()

    if not args.candidate.exists():
        print(json.dumps({"error": f"candidate path missing: {args.candidate}"}), file=sys.stderr)
        return 2
    if not args.reference.exists():
        print(json.dumps({"error": f"reference path missing: {args.reference}"}), file=sys.stderr)
        return 2

    checks: list[dict[str, Any]] = []
    ref_map, ref_err = load_csv_map(args.reference)
    checks.append(
        check(
            "reference_readable",
            "Reference CSV readable",
            ref_err is None,
            ref_err or f"{len(ref_map)} rows",
        )
    )
    cand_map, cand_err = load_csv_map(args.candidate)
    checks.append(
        check(
            "candidate_readable",
            "Candidate CSV readable",
            cand_err is None,
            cand_err or f"{len(cand_map)} rows",
        )
    )

    if ref_err is None and cand_err is None:
        checks.extend(compare(cand_map, ref_map))

    passed = all(c["passed"] for c in checks)
    result = {
        "passed": passed,
        "summary": (
            f"Passed — {len(ref_map)} settlement group(s) match reference"
            if passed
            else "Failed — settlement summary differs from reference"
        ),
        "checks": checks,
    }
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
