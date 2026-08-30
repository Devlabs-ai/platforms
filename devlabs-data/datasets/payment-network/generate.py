#!/usr/bin/env python3
"""Generate payment-network playground datasets (txns + dim tables) → MinIO.

Layout under s3://devlabs-data/datasets/payment-network/:
  txns/{10k,100k,1m,10m,50m,1m-skew-mcc,50m-skew-mcc,100m-skew-mcc,
        1m-skew-key60,50m-skew-key60,1m-skew-key75,50m-skew-key75,
        100m-skew-key75}/
  dims/{country,mcc,currency,response_code,entry_mode,acquirer}/

Usage:
  python3 generate.py                  # local ./_out/
  python3 generate.py --upload         # also push to MinIO
  python3 generate.py --sizes 100k,1m --upload
  python3 generate.py --sizes 100m-skew-mcc --upload --skip-dims

Schemas:
  legacy (12 cols): card_token + core payment fields — default
  wide (15 cols):   card_number + merchant_country + auth_code + settled —
                    100m* and 50m-skew-key75

Skewed MCC facts (*-skew-mcc):
  Hot MCC (default 5411) gets a size-specific fraction; remaining mass is split
  evenly across the other 29 MCCs. Override hot code with SKEW_MCC.

Skewed join-key facts (*-skew-keyNN):
  NN percent of rows are forced onto one (mcc, country_code, entry_mode) triple
  — default (5411, US, chip). The remainder is drawn from the ordinary marginals
  with a uniform MCC, so exactly one key is pathological. Override the triple
  with SKEW_MCC / SKEW_COUNTRY / SKEW_ENTRY.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import zlib
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

FAMILY = "payment-network"
DEFAULT_PREFIX = f"datasets/{FAMILY}"
DATA_SEED = int(os.environ.get("DATA_SEED", "42"))
CHUNK = int(os.environ.get("TXN_CHUNK", "250000"))
# Hot MCC for skew variants (must exist in MCCS).
SKEW_MCC = os.environ.get("SKEW_MCC", "5411")
# Global default; per-size overrides in SIZE_MCC_HOT below.
SKEW_HOT_FRACTION = float(os.environ.get("SKEW_HOT_FRACTION", "0.90"))
# Hot join-key triple for *-skew-keyNN variants.
SKEW_COUNTRY = os.environ.get("SKEW_COUNTRY", "US")
SKEW_ENTRY = os.environ.get("SKEW_ENTRY", "chip")
# Trailing -skew-key60 → 60% of rows on the hot triple.
KEY_SKEW_RE = re.compile(r"-skew-key(\d{1,2})$")

SIZE_SPECS = {
    "10k": int(os.environ.get("TXN_10K", "10000")),
    "20k-skew-key75": int(os.environ.get("TXN_20K_SKEW_KEY75", "20000")),
    "100k": int(os.environ.get("TXN_100K", "100000")),
    "1m": int(os.environ.get("TXN_1M", "1000000")),
    "10m": int(os.environ.get("TXN_10M", "10000000")),
    "50m": int(os.environ.get("TXN_50M", "50000000")),
    "1m-skew-mcc": int(os.environ.get("TXN_1M_SKEW_MCC", "1000000")),
    "50m-skew-mcc": int(os.environ.get("TXN_50M_SKEW_MCC", "50000000")),
    "100m-skew-mcc": int(os.environ.get("TXN_100M_SKEW_MCC", "100000000")),
    "1m-skew-key60": int(os.environ.get("TXN_1M_SKEW_KEY60", "1000000")),
    "50m-skew-key60": int(os.environ.get("TXN_50M_SKEW_KEY60", "50000000")),
    "1m-skew-key75": int(os.environ.get("TXN_1M_SKEW_KEY75", "1000000")),
    "50m-skew-key75": int(os.environ.get("TXN_50M_SKEW_KEY75", "50000000")),
    "100m-skew-key75": int(os.environ.get("TXN_100M_SKEW_KEY75", "100000000")),
}

# Extra wide-schema drops beyond the 100m* default. 50m-skew-key75 shares the
# 100m drop's shape so a plan can be shaped on half the scan and then run
# unchanged against the full one.
WIDE_SIZES = frozenset({"50m-skew-key75", "20k-skew-key75"})

# Per-size hot-MCC fraction for *-skew-mcc (env SKEW_HOT_FRACTION overrides all).
SIZE_MCC_HOT = {
    "1m-skew-mcc": 0.90,
    "50m-skew-mcc": 0.90,
    "100m-skew-mcc": 0.80,
}

TXN_SCHEMA = pa.schema([
    ("txn_id", pa.string()),
    ("txn_ts", pa.timestamp("us")),
    ("card_token", pa.string()),
    ("amount", pa.decimal128(14, 2)),
    ("currency_code", pa.string()),
    ("country_code", pa.string()),
    ("mcc", pa.string()),
    ("merchant_id", pa.string()),
    ("acquirer_bin", pa.string()),
    ("response_code", pa.string()),
    ("entry_mode", pa.string()),
    ("channel", pa.string()),
])

# Wider fact schema for 100m* drops (15 columns).
TXN_SCHEMA_WIDE = pa.schema([
    ("txn_id", pa.string()),
    ("txn_ts", pa.timestamp("us")),
    ("card_number", pa.string()),
    ("amount", pa.decimal128(14, 2)),
    ("currency_code", pa.string()),
    ("country_code", pa.string()),
    ("mcc", pa.string()),
    ("merchant_id", pa.string()),
    ("acquirer_bin", pa.string()),
    ("response_code", pa.string()),
    ("entry_mode", pa.string()),
    ("channel", pa.string()),
    ("merchant_country", pa.string()),
    ("auth_code", pa.string()),
    ("settled", pa.bool_()),
])


def schema_for_size(size: str) -> pa.Schema:
    wide = size.startswith("100m") or size in WIDE_SIZES
    return TXN_SCHEMA_WIDE if wide else TXN_SCHEMA


def hot_fraction_for_size(size: str) -> float:
    if os.environ.get("SKEW_HOT_FRACTION"):
        return SKEW_HOT_FRACTION
    return SIZE_MCC_HOT.get(size, SKEW_HOT_FRACTION)

# ── Dimension seeds (opcode / directory tables) ──────────────────────────────

COUNTRIES = [
    ("US", "United States", "NA", "low"),
    ("CA", "Canada", "NA", "low"),
    ("GB", "United Kingdom", "EU", "low"),
    ("DE", "Germany", "EU", "low"),
    ("FR", "France", "EU", "low"),
    ("IE", "Ireland", "EU", "low"),
    ("NL", "Netherlands", "EU", "low"),
    ("ES", "Spain", "EU", "medium"),
    ("IT", "Italy", "EU", "medium"),
    ("PL", "Poland", "EU", "medium"),
    ("SE", "Sweden", "EU", "low"),
    ("CH", "Switzerland", "EU", "low"),
    ("AU", "Australia", "APAC", "low"),
    ("NZ", "New Zealand", "APAC", "low"),
    ("JP", "Japan", "APAC", "low"),
    ("SG", "Singapore", "APAC", "low"),
    ("HK", "Hong Kong", "APAC", "medium"),
    ("IN", "India", "APAC", "medium"),
    ("BR", "Brazil", "LATAM", "medium"),
    ("MX", "Mexico", "LATAM", "medium"),
    ("AR", "Argentina", "LATAM", "high"),
    ("ZA", "South Africa", "MEA", "medium"),
    ("AE", "United Arab Emirates", "MEA", "medium"),
    ("NG", "Nigeria", "MEA", "high"),
    ("TR", "Türkiye", "MEA", "medium"),
]

CURRENCIES = [
    ("USD", "US Dollar", 2),
    ("CAD", "Canadian Dollar", 2),
    ("GBP", "Pound Sterling", 2),
    ("EUR", "Euro", 2),
    ("AUD", "Australian Dollar", 2),
    ("NZD", "New Zealand Dollar", 2),
    ("JPY", "Yen", 0),
    ("SGD", "Singapore Dollar", 2),
    ("HKD", "Hong Kong Dollar", 2),
    ("INR", "Indian Rupee", 2),
    ("BRL", "Brazilian Real", 2),
    ("MXN", "Mexican Peso", 2),
    ("CHF", "Swiss Franc", 2),
    ("SEK", "Swedish Krona", 2),
    ("PLN", "Polish Zloty", 2),
    ("AED", "UAE Dirham", 2),
    ("ZAR", "South African Rand", 2),
    ("TRY", "Turkish Lira", 2),
    ("NGN", "Nigerian Naira", 2),
    ("ARS", "Argentine Peso", 2),
]

# Weighted MCC sample (real codes, short labels)
MCCS = [
    ("5411", "Grocery Stores", "retail"),
    ("5812", "Eating Places", "restaurant"),
    ("5814", "Fast Food", "restaurant"),
    ("5541", "Service Stations", "fuel"),
    ("5542", "Automated Fuel", "fuel"),
    ("4111", "Local Transit", "travel"),
    ("4511", "Airlines", "travel"),
    ("7011", "Hotels", "travel"),
    ("5311", "Department Stores", "retail"),
    ("5691", "Clothing Stores", "retail"),
    ("5732", "Electronics", "retail"),
    ("5999", "Miscellaneous Retail", "retail"),
    ("6011", "ATM Cash Advance", "cash"),
    ("6010", "Manual Cash", "cash"),
    ("4900", "Utilities", "services"),
    ("6300", "Insurance", "services"),
    ("7995", "Betting", "high_risk"),
    ("5921", "Package Stores — Beer Wine Liquor", "high_risk"),
    ("6051", "Quasi Cash", "high_risk"),
    ("4829", "Wire Transfer Money Orders", "high_risk"),
    ("7399", "Business Services NEC", "services"),
    ("8999", "Professional Services", "services"),
    ("8011", "Doctors", "healthcare"),
    ("8021", "Dentists", "healthcare"),
    ("5912", "Drug Stores", "healthcare"),
    ("7832", "Motion Picture Theaters", "entertainment"),
    ("7922", "Ticket Agencies", "entertainment"),
    ("4121", "Taxicabs Limousines", "travel"),
    ("4784", "Tolls", "travel"),
    ("9402", "Postal Services", "government"),
]

RESPONSE_CODES = [
    ("00", "Approved", True),
    ("01", "Refer to issuer", False),
    ("05", "Do not honor", False),
    ("14", "Invalid card number", False),
    ("51", "Insufficient funds", False),
    ("54", "Expired card", False),
    ("57", "Transaction not permitted", False),
    ("61", "Exceeds withdrawal limit", False),
    ("62", "Restricted card", False),
    ("65", "Exceeds frequency limit", False),
    ("91", "Issuer unavailable", False),
    ("96", "System malfunction", False),
]

ENTRY_MODES = [
    ("chip", "EMV chip read"),
    ("contactless", "NFC / contactless"),
    ("magstripe", "Magnetic stripe"),
    ("ecom", "Card-not-present e-commerce"),
    ("moto", "Mail / telephone order"),
    ("manual", "Key-entered"),
    ("token", "Network token"),
]

ACQUIRERS = [
    ("400001", "Northstar Acquiring", "US"),
    ("400002", "Maple Pay", "CA"),
    ("400003", "Thames Merchant Services", "GB"),
    ("400004", "Rhein Acquirer", "DE"),
    ("400005", "Seine Card Services", "FR"),
    ("400006", "Iberia Merchant Bank", "ES"),
    ("400007", "Pacific Gate Acquiring", "AU"),
    ("400008", "Sakura Merchant", "JP"),
    ("400009", "Lion City Acquirer", "SG"),
    ("400010", "Ganges Acceptance", "IN"),
    ("400011", "Andes Pay", "BR"),
    ("400012", "Aztec Merchant Services", "MX"),
    ("400013", "Alpine Acquiring", "CH"),
    ("400014", "Gulf Route Payments", "AE"),
    ("400015", "Cape Clearing", "ZA"),
]

CHANNELS = ["pos", "atm", "ecom", "moto", "mobile"]

# Base sampling weights (mild skew: US/GB/DE heavier, chip/contactless dominant).
COUNTRY_W = np.array(
    [0.28, 0.06, 0.10, 0.08, 0.07] + [0.41 / (len(COUNTRIES) - 5)] * (len(COUNTRIES) - 5)
)
COUNTRY_W = COUNTRY_W / COUNTRY_W.sum()
ENTRY_W = np.array([0.35, 0.30, 0.05, 0.22, 0.03, 0.02, 0.03])

# Country → default currency (rough)
COUNTRY_CCY = {
    "US": "USD", "CA": "CAD", "GB": "GBP", "DE": "EUR", "FR": "EUR", "IE": "EUR",
    "NL": "EUR", "ES": "EUR", "IT": "EUR", "PL": "PLN", "SE": "SEK", "CH": "CHF",
    "AU": "AUD", "NZ": "NZD", "JP": "JPY", "SG": "SGD", "HK": "HKD", "IN": "INR",
    "BR": "BRL", "MX": "MXN", "AR": "ARS", "ZA": "ZAR", "AE": "AED", "NG": "NGN",
    "TR": "TRY",
}


def _minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        endpoint = os.environ.get(
            "MINIO_ENDPOINT",
            "http://minio.minio.svc.cluster.local:9000",
        )
    host = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "spark"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "spark-s3-change-me"),
        secure=endpoint.startswith("https"),
    )


def write_table(table: pa.Table, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    pq.write_table(table, path, compression="snappy", row_group_size=100_000)
    (out_dir / "_SUCCESS").write_text("")
    return path


def build_interchange_rates() -> pa.Table:
    """Scheme interchange rate card: one row per (mcc, country, entry_mode, week).

    Joined from the fact on (mcc, country_code, entry_mode) plus a date-range
    predicate. Deliberately keyed so `mcc` dominates the equi-key — adding more
    key columns would fan the hot MCC out and erase the skew.
    """
    import datetime as _dt

    base_bps = {
        "retail": 130, "restaurant": 155, "fuel": 95, "travel": 180,
        "cash": 60, "services": 145, "high_risk": 235, "healthcare": 120,
        "entertainment": 165, "government": 55,
    }
    entry_adj = {
        "chip": 0, "contactless": -5, "magstripe": 15, "ecom": 45,
        "moto": 60, "manual": 55, "token": -10,
    }
    risk_adj = {"low": 0, "medium": 12, "high": 30}
    mcc_cat = {m[0]: m[2] for m in MCCS}
    country_risk = {c[0]: c[3] for c in COUNTRIES}

    period_start = _dt.date(2024, 1, 1)
    # Weekly schedules well past the fact window: rate cards carry historical and
    # future-dated versions, and the row count keeps the table non-broadcastable.
    n_weeks = int(os.environ.get("RATE_WEEKS", "130"))
    epoch = _dt.date(1970, 1, 1)

    rate_ids, mccs, countries, entries = [], [], [], []
    eff_from, eff_to, bps, fixed = [], [], [], []
    prog_code, prog_name = [], []

    idx = 0
    for w in range(n_weeks):
        start = period_start + _dt.timedelta(days=7 * w)
        end = (
            _dt.date(2099, 12, 31)
            if w == n_weeks - 1
            else start + _dt.timedelta(days=6)
        )
        for mcc, _desc, cat in MCCS:
            for cc, cname, _region, risk in COUNTRIES:
                for em, _emdesc in ENTRY_MODES:
                    idx += 1
                    rate = base_bps[cat] + entry_adj[em] + risk_adj[risk] + (w % 5) - 2
                    rate_ids.append(f"IC{idx:09d}")
                    mccs.append(mcc)
                    countries.append(cc)
                    entries.append(em)
                    eff_from.append((start - epoch).days)
                    eff_to.append((end - epoch).days)
                    bps.append(rate)
                    fixed.append(Decimal(5 + (idx % 8)) / Decimal(1000))
                    prog_code.append(f"{cat.upper()[:4]}-{em.upper()[:4]}-W{w:02d}")
                    prog_name.append(
                        f"{cname} {cat.replace('_', ' ').title()} {em} programme, "
                        f"week {w:02d} effective schedule — settlement interchange "
                        f"published by scheme for acquirer billing and reconciliation"
                    )

    return pa.table(
        {
            "rate_id": rate_ids,
            "mcc": mccs,
            "country_code": countries,
            "entry_mode": entries,
            "effective_from": pa.array(eff_from, type=pa.date32()),
            "effective_to": pa.array(eff_to, type=pa.date32()),
            "rate_bps": pa.array(bps, type=pa.int32()),
            "fixed_fee": pa.array(fixed, type=pa.decimal128(6, 4)),
            "program_code": prog_code,
            "program_name": prog_name,
        }
    )


def build_dims() -> dict[str, pa.Table]:
    country = pa.table({
        "country_code": [c[0] for c in COUNTRIES],
        "country_name": [c[1] for c in COUNTRIES],
        "region": [c[2] for c in COUNTRIES],
        "risk_tier": [c[3] for c in COUNTRIES],
    })
    mcc = pa.table({
        "mcc": [m[0] for m in MCCS],
        "mcc_description": [m[1] for m in MCCS],
        "category": [m[2] for m in MCCS],
    })
    currency = pa.table({
        "currency_code": [c[0] for c in CURRENCIES],
        "currency_name": [c[1] for c in CURRENCIES],
        "minor_units": pa.array([c[2] for c in CURRENCIES], type=pa.int8()),
    })
    response = pa.table({
        "response_code": [r[0] for r in RESPONSE_CODES],
        "meaning": [r[1] for r in RESPONSE_CODES],
        "is_approved": [r[2] for r in RESPONSE_CODES],
    })
    entry = pa.table({
        "entry_mode": [e[0] for e in ENTRY_MODES],
        "description": [e[1] for e in ENTRY_MODES],
    })
    acquirer = pa.table({
        "acquirer_bin": [a[0] for a in ACQUIRERS],
        "acquirer_name": [a[1] for a in ACQUIRERS],
        "country_code": [a[2] for a in ACQUIRERS],
    })
    return {
        "country": country,
        "mcc": mcc,
        "currency": currency,
        "response_code": response,
        "entry_mode": entry,
        "acquirer": acquirer,
        "interchange_rate": build_interchange_rates(),
    }


def mcc_probs_for_size(size: str) -> np.ndarray | None:
    """Return MCC sampling weights, or None for uniform.

    Skew sizes (`*-skew-mcc`): hot MCC gets a size-specific fraction; the rest
    share the remainder evenly across the other codes.
    """
    if not size.endswith("-skew-mcc"):
        return None
    codes = [m[0] for m in MCCS]
    if SKEW_MCC not in codes:
        raise SystemExit(f"SKEW_MCC={SKEW_MCC!r} not in MCCS ({codes})")
    hot_p = hot_fraction_for_size(size)
    if not (0.0 < hot_p < 1.0):
        raise SystemExit(f"hot MCC fraction must be in (0,1), got {hot_p}")
    hot_i = codes.index(SKEW_MCC)
    n_other = len(codes) - 1
    other_p = (1.0 - hot_p) / n_other
    probs = np.full(len(codes), other_p, dtype=np.float64)
    probs[hot_i] = hot_p
    return probs


def hot_key_target_for_size(size: str) -> float | None:
    """Target share of rows on the hot (mcc, country, entry) triple, or None."""
    match = KEY_SKEW_RE.search(size)
    if match is None:
        return None
    target = int(match.group(1)) / 100.0
    if not (0.0 < target < 1.0):
        raise SystemExit(f"hot key share must be in (0,1), got {target}")
    return target


def hot_key_force_prob(target: float, mcc_p: np.ndarray | None = None) -> float:
    """Per-row probability of overwriting the key so the triple lands on `target`.

    Rows not overwritten still hit the triple by chance, so forcing `target`
    outright would overshoot. Solve p·1 + (1−p)·base = target instead.
    """
    codes = [m[0] for m in MCCS]
    countries = [c[0] for c in COUNTRIES]
    entries = [e[0] for e in ENTRY_MODES]
    for value, domain, label in (
        (SKEW_MCC, codes, "SKEW_MCC"),
        (SKEW_COUNTRY, countries, "SKEW_COUNTRY"),
        (SKEW_ENTRY, entries, "SKEW_ENTRY"),
    ):
        if value not in domain:
            raise SystemExit(f"{label}={value!r} not in {domain}")
    p_mcc = float(mcc_p[codes.index(SKEW_MCC)]) if mcc_p is not None else 1.0 / len(codes)
    base = p_mcc * float(COUNTRY_W[countries.index(SKEW_COUNTRY)])
    base *= float(ENTRY_W[entries.index(SKEW_ENTRY)])
    if target <= base:
        raise SystemExit(f"target {target} below natural share {base:.6f}")
    return (target - base) / (1.0 - base)


def _luhn_check_digit(body: str) -> str:
    """Return Luhn check digit for a numeric body (no check digit yet)."""
    total = 0
    # Double every second digit from the right, including where check digit sits.
    reverse = body[::-1]
    for i, ch in enumerate(reverse, start=1):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - (total % 10)) % 10)


def _synthetic_card_pool(pool: int) -> np.ndarray:
    """Stable synthetic Visa-like PANs (Luhn-valid, not real cards)."""
    pool = max(10_000, min(pool, 200_000))
    bodies = []
    for i in range(pool):
        body = f"400000{i:09d}"[:15]
        bodies.append(body + _luhn_check_digit(body))
    return np.array(bodies, dtype=object)


def _build_txn_chunk(
    start: int,
    n: int,
    rng: np.random.Generator,
    *,
    mcc_p: np.ndarray | None = None,
    schema: pa.Schema = TXN_SCHEMA,
    card_pool: np.ndarray | None = None,
    hot_key_p: float | None = None,
) -> pa.Table:
    country_codes = np.array([c[0] for c in COUNTRIES])
    countries = rng.choice(country_codes, size=n, p=COUNTRY_W)

    mcc_codes = np.array([m[0] for m in MCCS])
    mccs = rng.choice(mcc_codes, size=n, p=mcc_p)

    resp_codes = np.array([r[0] for r in RESPONSE_CODES])
    # ~88% approved
    resp_w = np.array([0.88] + [0.12 / (len(RESPONSE_CODES) - 1)] * (len(RESPONSE_CODES) - 1))
    responses = rng.choice(resp_codes, size=n, p=resp_w)

    entry_codes = np.array([e[0] for e in ENTRY_MODES])
    entries = rng.choice(entry_codes, size=n, p=ENTRY_W)

    if hot_key_p:
        hot = rng.random(n) < hot_key_p
        mccs[hot] = SKEW_MCC
        countries[hot] = SKEW_COUNTRY
        entries[hot] = SKEW_ENTRY

    acq_bins = np.array([a[0] for a in ACQUIRERS])
    acquirers = rng.choice(acq_bins, size=n)

    channels = rng.choice(np.array(CHANNELS), size=n, p=[0.45, 0.08, 0.35, 0.05, 0.07])

    currencies = np.array([COUNTRY_CCY.get(str(c), "USD") for c in countries])

    # Amounts: log-normal-ish cents
    cents = np.clip(rng.lognormal(mean=6.5, sigma=1.1, size=n).astype(np.int64), 1, 5_000_000)
    amounts = pa.array(
        [Decimal(int(c)) / Decimal(100) for c in cents],
        type=pa.decimal128(14, 2),
    )

    # 2024-01-01T00:00:00Z in μs + random offset over ~180 days
    base_us = np.datetime64("2024-01-01T00:00:00", "us")
    span_us = int(timedelta(days=180).total_seconds() * 1_000_000)
    offsets_us = rng.integers(0, span_us, size=n, dtype=np.int64)
    ts = base_us + offsets_us.astype("timedelta64[us]")

    txn_ids = np.array([f"TXN{start + i:012d}" for i in range(n)])
    merchants = np.array([f"MID{int(x):07d}" for x in rng.integers(1, 50_000, size=n)])

    cols: dict = {
        "txn_id": txn_ids,
        "txn_ts": ts,
        "amount": amounts,
        "currency_code": currencies,
        "country_code": countries,
        "mcc": mccs,
        "merchant_id": merchants,
        "acquirer_bin": acquirers,
        "response_code": responses,
        "entry_mode": entries,
        "channel": channels,
    }

    if schema is TXN_SCHEMA_WIDE:
        if card_pool is None:
            card_pool = _synthetic_card_pool(80_000)
        cols["card_number"] = card_pool[rng.integers(0, len(card_pool), size=n)]
        # ~12% cross-border: merchant country differs from card country
        cross = rng.random(n) < 0.12
        merchant_country = countries.copy()
        alt = rng.choice(country_codes, size=int(cross.sum()))
        merchant_country[cross] = alt
        cols["merchant_country"] = merchant_country
        approved = responses == "00"
        auth_codes = np.full(n, "", dtype=object)
        auth_codes[approved] = np.array(
            [f"{int(x):06d}" for x in rng.integers(0, 1_000_000, size=int(approved.sum()))],
            dtype=object,
        )
        cols["auth_code"] = auth_codes
        # Declines never settle; ~94% of approved settle
        settled = np.zeros(n, dtype=bool)
        settled[approved] = rng.random(int(approved.sum())) < 0.94
        cols["settled"] = settled
    else:
        card_n = max(10_000, min(80_000, n))
        card_idx = rng.integers(1, card_n + 1, size=n)
        cols["card_token"] = np.array([f"tok_{int(x):08d}" for x in card_idx])

    return pa.table(cols, schema=schema)


def write_txns(
    n_rows: int,
    out_dir: Path,
    seed: int,
    *,
    mcc_p: np.ndarray | None = None,
    schema: pa.Schema = TXN_SCHEMA,
    hot_key_p: float | None = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-00000.parquet"
    rng = np.random.default_rng(seed)
    card_pool = _synthetic_card_pool(80_000) if schema is TXN_SCHEMA_WIDE else None
    writer: pq.ParquetWriter | None = None
    written = 0
    next_id = 1
    try:
        while written < n_rows:
            n = min(CHUNK, n_rows - written)
            table = _build_txn_chunk(
                next_id, n, rng, mcc_p=mcc_p, schema=schema, card_pool=card_pool,
                hot_key_p=hot_key_p,
            )
            if writer is None:
                writer = pq.ParquetWriter(path, schema, compression="snappy")
            writer.write_table(table)
            written += n
            next_id += n
            print(f"  txns  {written:,}/{n_rows:,}", flush=True)
    finally:
        if writer is not None:
            writer.close()
    (out_dir / "_SUCCESS").write_text("")
    return path


def upload_dir(local_dir: Path, key_prefix: str) -> None:
    client = _minio_client()
    bucket = os.environ.get("MINIO_BUCKET", os.environ.get("S3_BUCKET", "devlabs-data"))
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        key = f"{key_prefix.rstrip('/')}/{rel}"
        client.fput_object(bucket, key, str(path))
        print(f"PUT  s3://{bucket}/{key} ({path.stat().st_size} bytes)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument(
        "--sizes",
        default="10k,100k,1m,10m,50m",
        help=(
            "Comma list of txn sizes (10k,…,50m,1m-skew-mcc,50m-skew-mcc,"
            "100m-skew-mcc,1m-skew-key60,50m-skew-key60,1m-skew-key75,"
            "50m-skew-key75,20k-skew-key75,100m-skew-key75)"
        ),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "_out",
    )
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--skip-dims", action="store_true")
    args = parser.parse_args()

    sizes = [s.strip() for s in args.sizes.split(",") if s.strip()]
    for s in sizes:
        if s not in SIZE_SPECS:
            raise SystemExit(f"unknown size {s!r}; choose from {list(SIZE_SPECS)}")

    print(f"BUILD  family={FAMILY} seed={DATA_SEED} sizes={sizes}")
    args.out.mkdir(parents=True, exist_ok=True)

    if not args.skip_dims:
        dims = build_dims()
        for name, table in dims.items():
            ddir = args.out / "dims" / name
            path = write_table(table, ddir)
            print(f"WRITE  dims/{name} rows={table.num_rows} → {path}")

    for size in sizes:
        n = SIZE_SPECS[size]
        tdir = args.out / "txns" / size
        schema = schema_for_size(size)
        mcc_p = mcc_probs_for_size(size)
        hot_target = hot_key_target_for_size(size)
        hot_key_p = hot_key_force_prob(hot_target, mcc_p) if hot_target else None
        if hot_target is not None:
            print(
                f"BUILD  txns/{size} rows={n:,} schema={schema_label(schema)} "
                f"key_skew hot=({SKEW_MCC},{SKEW_COUNTRY},{SKEW_ENTRY}) "
                f"target={hot_target:.2f} force_p={hot_key_p:.6f}"
            )
        elif mcc_p is not None:
            hot_p = hot_fraction_for_size(size)
            print(
                f"BUILD  txns/{size} rows={n:,} schema={schema_label(schema)} "
                f"mcc_skew hot={SKEW_MCC} p={hot_p:.2f} "
                f"others={len(MCCS) - 1}×{(1.0 - hot_p) / (len(MCCS) - 1):.6f}"
            )
        else:
            print(f"BUILD  txns/{size} rows={n:,} schema={schema_label(schema)}")
        path = write_txns(
            n,
            tdir,
            # crc32, not hash(): str hashing is salted per process, so hash()
            # made every rebuild of the same size produce different rows.
            DATA_SEED + zlib.crc32(size.encode()) % 10_000,
            mcc_p=mcc_p,
            schema=schema,
            hot_key_p=hot_key_p,
        )
        print(f"WRITE  txns/{size} → {path} ({path.stat().st_size} bytes)")

    if args.upload:
        if not args.skip_dims:
            upload_dir(args.out / "dims", f"{args.prefix}/dims")
        for size in sizes:
            upload_dir(args.out / "txns" / size, f"{args.prefix}/txns/{size}")
        print(f"UPLOAD_OK  s3a://…/{args.prefix}/")
    else:
        print("NOTE  pass --upload to push to MinIO")
    return 0


def schema_label(schema: pa.Schema) -> str:
    return "wide15" if schema is TXN_SCHEMA_WIDE else "legacy12"


if __name__ == "__main__":
    sys.exit(main())
