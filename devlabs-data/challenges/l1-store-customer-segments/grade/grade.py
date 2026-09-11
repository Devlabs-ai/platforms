#!/usr/bin/env python3
"""Grade Challenge 8 — Approximate unique customers and percentiles.

Functional: compare candidate Parquet to the golden row by row (after a
stable sort on store_id). approx_unique_customers allows 2x rsd (0.10)
relative error. Percentiles allow a 0.01 decimal tolerance.

Usage:
  python grade.py --candidate <dir> --reference <dir>
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

KEY = "store_id"
SKIP_PARTS = ("_temporary", ".spark-staging")
APPROX_RSD = 0.05
PERCENTILE_TOL = Decimal("0.01")
PERCENTILE_COLS = ("revenue_p25", "revenue_p50", "revenue_p75")


def check(cid: str, label: str, passed: bool, detail: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"id": cid, "label": label, "passed": passed}
    if detail:
        out["detail"] = detail
    return out


def find_parquet(path: Path) -> list[Path]:
    if path.is_file() and path.suffix == ".parquet":
        return [path]
    files = [
        p
        for p in path.rglob("*.parquet")
        if p.is_file()
        and not p.name.startswith(".")
        and not any(part in SKIP_PARTS for part in p.parts)
    ]
    return sorted(files)


def cell(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return format(Decimal(str(value)), "f")
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def load_rows(path: Path) -> tuple[list[str], list[dict[str, Any]], str | None]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        return [], [], "pyarrow is required to grade Parquet (set DEVLABS_GRADE_PYTHON)"

    files = find_parquet(path)
    if not files:
        return [], [], f"no Parquet files under {path}"

    tables = []
    for f in files:
        try:
            tables.append(pq.read_table(f))
        except Exception as exc:  # noqa: BLE001
            return [], [], f"{f.name}: {exc}"

    table = tables[0]
    for extra in tables[1:]:
        table = table.combine_chunks() if hasattr(table, "combine_chunks") else table
        extra = extra.combine_chunks() if hasattr(extra, "combine_chunks") else extra
        if table.schema.names != extra.schema.names:
            return [], [], (
                f"column mismatch across parts: {table.schema.names} vs {extra.schema.names}"
            )
        import pyarrow as pa  # type: ignore

        table = pa.concat_tables([table, extra], promote_options="default")

    columns = list(table.column_names)
    if KEY not in columns:
        return columns, [], f"missing grade key {KEY}"

    rows: list[dict[str, Any]] = []
    for raw in table.to_pylist():
        rows.append({col: cell(raw.get(col)) for col in columns})

    def sort_key(row: dict[str, Any]) -> tuple[int, Any]:
        value = row.get(KEY)
        if value is None:
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, value)
        try:
            return (0, int(value))
        except (TypeError, ValueError):
            return (0, str(value))

    rows.sort(key=sort_key)
    return columns, rows, None


def decimalish(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rows_equal(ref: dict[str, Any], cand: dict[str, Any], cols: list[str]) -> list[str]:
    diffs: list[str] = []
    for col in cols:
        left, right = ref.get(col), cand.get(col)
        if col == "approx_unique_customers":
            gold = as_int(left) or 0
            got = as_int(right) or 0
            if gold > 0:
                relative = abs(got - gold) / gold
                if relative > APPROX_RSD * 2:
                    diffs.append(
                        f"{col}: expected~{gold} got={got} (rel_err={relative:.3f})"
                    )
            elif got != gold:
                diffs.append(f"{col}: expected={gold} got={got}")
            continue
        if col in PERCENTILE_COLS:
            a, b = decimalish(left), decimalish(right)
            if a is None or b is None or abs(a - b) > PERCENTILE_TOL:
                diffs.append(f"{col}: expected={left!r} got={right!r}")
            continue
        if left != right:
            diffs.append(f"{col}: expected={left!r} got={right!r}")
    return diffs


def compare_rows(
    ref_cols: list[str],
    ref_rows: list[dict[str, Any]],
    cand_cols: list[str],
    cand_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    cols_ok = ref_cols == cand_cols
    checks.append(
        check(
            "columns_match",
            "Column names and order match",
            cols_ok,
            f"expected={ref_cols} got={cand_cols}" if not cols_ok else f"{len(ref_cols)} columns",
        )
    )
    if not cols_ok:
        return checks

    count_ok = len(ref_rows) == len(cand_rows)
    checks.append(
        check(
            "row_count",
            "Row count matches",
            count_ok,
            f"expected={len(ref_rows)} got={len(cand_rows)}",
        )
    )

    mismatches = 0
    examples: list[str] = []
    limit = min(len(ref_rows), len(cand_rows))
    for i in range(limit):
        diffs = rows_equal(ref_rows[i], cand_rows[i], ref_cols)
        if not diffs:
            continue
        mismatches += 1
        if len(examples) >= 5:
            continue
        key = ref_rows[i].get(KEY)
        examples.append(f"{KEY}={key} ({'; '.join(diffs[:3])})")

    extra = max(0, len(cand_rows) - len(ref_rows))
    missing = max(0, len(ref_rows) - len(cand_rows))
    line_ok = mismatches == 0 and extra == 0 and missing == 0
    detail = f"compared={limit} mismatch={mismatches} missing={missing} extra={extra}"
    if examples:
        detail += f"; e.g. {'; '.join(examples)}"
    checks.append(
        check(
            "rows_match",
            "Rows match expected (approx unique customers within tolerance)",
            line_ok,
            detail,
        )
    )
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Grade store-customer-segments Parquet")
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    args = parser.parse_args()

    if not args.candidate.exists():
        print(json.dumps({"error": f"candidate path missing: {args.candidate}"}), file=sys.stderr)
        return 2
    if not args.reference.exists():
        print(json.dumps({"error": f"reference path missing: {args.reference}"}), file=sys.stderr)
        return 2

    ref_cols, ref_rows, ref_err = load_rows(args.reference)
    cand_cols, cand_rows, cand_err = load_rows(args.candidate)

    func_checks: list[dict[str, Any]] = [
        check(
            "reference_readable",
            "Reference Parquet readable",
            ref_err is None,
            ref_err or f"{len(ref_rows)} rows",
        ),
        check(
            "candidate_readable",
            "Candidate Parquet readable",
            cand_err is None,
            cand_err or f"{len(cand_rows)} rows",
        ),
    ]
    if ref_err is None and cand_err is None:
        func_checks.extend(compare_rows(ref_cols, ref_rows, cand_cols, cand_rows))

    functional_passed = all(c["passed"] for c in func_checks)
    if functional_passed:
        summary = f"Functional passed — {len(ref_rows)} rows match expected"
    else:
        count = next((c for c in func_checks if c.get("id") == "row_count" and not c.get("passed")), None)
        rows = next((c for c in func_checks if c.get("id") == "rows_match" and not c.get("passed")), None)
        cols = next((c for c in func_checks if c.get("id") == "columns_match" and not c.get("passed")), None)
        if cols and cols.get("detail"):
            summary = f"Functional failed — columns do not match ({cols['detail']})"
        elif count and count.get("detail"):
            summary = f"Functional failed — row count does not match ({count['detail']})"
        elif rows:
            short = str(rows.get("detail") or "").split("; e.g.")[0].strip()
            summary = (
                f"Functional failed — rows do not match expected ({short})"
                if short
                else "Functional failed — rows do not match expected"
            )
        else:
            summary = "Functional failed — output does not match expected Parquet"
    result = {
        "passed": functional_passed,
        "summary": summary,
        "sections": {
            "functional": {
                "passed": functional_passed,
                "checks": func_checks,
            },
            "performance": {
                "skipped": True,
                "detail": "This lab is functional only; time does not fail the grade",
            },
        },
    }
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
