#!/usr/bin/env python3
"""Generate mutual-fund snapshots for fast landing-page APIs.

This job keeps the legacy per-scheme table (`mf_performance_snapshot`) updated and
also writes snapshot-key payloads into `landing_page_snapshots` so API reads can use
precomputed JSON (same pattern as stockstable snapshots).
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import Json

DEFAULT_DATABASE_URL = (
    "postgresql://postgres:zKvzg4nrnigizkpbc1fN@"
    "budhi.cjwye68sytlr.ap-south-1.rds.amazonaws.com:5432/postgres"
)
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

UPSERT_RETRY_ATTEMPTS = int(os.getenv("SNAPSHOT_UPSERT_RETRY_ATTEMPTS", "5"))
UPSERT_RETRY_BASE_SECONDS = float(os.getenv("SNAPSHOT_UPSERT_RETRY_BASE_SECONDS", "0.25"))

CATEGORY_STRUCTURE = {
    "Equity": [
        "Flexi Cap",
        "International",
        "Large & Mid Cap",
        "Large Cap",
        "Mid Cap",
        "Multi Cap",
        "Sectoral",
        "Small Cap",
        "ELSS",
        "Thematic",
        "Value Oriented",
    ],
    "Debt": [
        "Banking and PSU",
        "Corporate Bond",
        "Credit Risk",
        "Dynamic Bond",
        "Fixed Maturity",
        "Floater",
        "Gilt",
        "Gilt with 10 Year Constant Duration",
        "Liquid",
        "Long Duration",
        "Low Duration",
        "Medium Duration",
        "Medium to Long Duration",
        "Money Market",
        "Overnight",
        "Short Duration",
        "Target Maturity",
        "Ultra Short Duration",
    ],
    "Hybrid": [
        "Aggressive Hybrid",
        "Arbitrage",
        "Balanced Hybrid",
        "Conservative Hybrid",
        "Dynamic Asset Allocation",
        "Equity Savings",
        "Multi Asset Allocation",
    ],
    "Commodities": ["Gold", "Silver"],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def pct_change(current: Any, base: Any) -> Optional[float]:
    if current is None or base is None:
        return None
    try:
        current_f = float(current)
        base_f = float(base)
    except Exception:
        return None
    if base_f == 0:
        return None
    return round(((current_f / base_f) - 1) * 100, 2)


def _extract_fund_house(scheme_name: str) -> str:
    if not scheme_name:
        return "Unknown"

    parts = scheme_name.split("-")
    first_part = parts[0].strip()

    if "Fund" in first_part:
        first_part = first_part.split("Fund")[0].strip()

    fund_house_map = {
        "Aditya Birla": "Aditya Birla Sun Life",
        "ABSL": "Aditya Birla Sun Life",
        "HDFC": "HDFC",
        "ICICI": "ICICI Prudential",
        "SBI": "SBI",
        "Axis": "Axis",
        "Kotak": "Kotak Mahindra",
        "UTI": "UTI",
        "DSP": "DSP",
        "Franklin": "Franklin Templeton",
        "Nippon": "Nippon India",
        "Tata": "Tata",
        "Mirae": "Mirae Asset",
        "Motilal": "Motilal Oswal",
        "IDFC": "IDFC",
        "L&T": "L&T",
        "Edelweiss": "Edelweiss",
        "Sundaram": "Sundaram",
        "PGIM": "PGIM India",
        "Quantum": "Quantum",
        "Baroda": "Baroda BNP Paribas",
        "Canara": "Canara Robeco",
        "HSBC": "HSBC",
        "Invesco": "Invesco",
        "JM": "JM Financial",
        "LIC": "LIC",
        "Mahindra": "Mahindra Manulife",
        "Navi": "Navi",
        "PPFAS": "PPFAS",
        "Quant": "Quant",
        "Samco": "Samco",
        "Shriram": "Shriram",
        "Union": "Union",
        "WhiteOak": "WhiteOak Capital",
    }

    for key, value in fund_house_map.items():
        if first_part.startswith(key):
            return value

    words = first_part.split()
    if len(words) >= 2:
        return " ".join(words[:2])
    return first_part


def _is_etf_scheme(scheme_name: str) -> bool:
    name = (scheme_name or "").strip().upper()
    if not name:
        return False
    # Exclude ETFs and exchange-traded NAV/inav ticker rows from MF snapshot.
    tokens = (
        " ETF",
        "ETF ",
        "-ETF",
        "ETF-",
        "ETFS",
        "EXCHANGE TRADED FUND",
        " INAV",
    )
    return any(token in name for token in tokens)


def _is_retryable_snapshot_error(exc: OperationalError) -> bool:
    code = getattr(exc, "pgcode", None)
    return code in {"40P01", "40001"}


def create_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mf_performance_snapshot (
                scheme_code BIGINT PRIMARY KEY,
                scheme_name TEXT,
                category TEXT,
                latest_nav NUMERIC,
                age_years NUMERIC,
                return_1d NUMERIC,
                return_30d NUMERIC,
                return_1y NUMERIC,
                since_inception_return NUMERIC,
                sparkline_1y JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE mf_performance_snapshot
            ADD COLUMN IF NOT EXISTS return_1d NUMERIC;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS landing_page_snapshots (
                snapshot_key TEXT PRIMARY KEY,
                snapshot_data JSONB NOT NULL,
                as_of_date DATE NULL,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_landing_page_snapshots_as_of
            ON landing_page_snapshots (as_of_date);
            """
        )
    conn.commit()


def upsert_landing_snapshot(conn, key: str, payload: Dict[str, Any], as_of_date: Optional[date]) -> None:
    statement = """
    INSERT INTO landing_page_snapshots (
        snapshot_key,
        snapshot_data,
        as_of_date,
        generated_at,
        updated_at
    )
    VALUES (%s, %s, %s, NOW(), NOW())
    ON CONFLICT (snapshot_key)
    DO UPDATE SET
        snapshot_data = EXCLUDED.snapshot_data,
        as_of_date = EXCLUDED.as_of_date,
        generated_at = NOW(),
        updated_at = NOW();
    """

    for attempt in range(UPSERT_RETRY_ATTEMPTS):
        try:
            with conn.cursor() as cur:
                cur.execute(statement, (key, Json(payload), as_of_date))
            return
        except OperationalError as exc:
            if not _is_retryable_snapshot_error(exc) or attempt == UPSERT_RETRY_ATTEMPTS - 1:
                raise
            sleep_seconds = UPSERT_RETRY_BASE_SECONDS * (2 ** attempt)
            print(
                f"Retryable DB error while upserting {key}. "
                f"Retry {attempt + 1}/{UPSERT_RETRY_ATTEMPTS} in {sleep_seconds:.2f}s"
            )
            time.sleep(sleep_seconds)


def fetch_base_rows(conn) -> List[Tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_nav AS (
                SELECT scheme_code, nav, nav_date
                FROM mf_latest_nav
            ),
            prev_nav AS (
                SELECT DISTINCT ON (h.scheme_code)
                    h.scheme_code,
                    h.nav
                FROM mf_nav_history h
                JOIN latest_nav l ON l.scheme_code = h.scheme_code
                WHERE h.nav_date < l.nav_date
                ORDER BY h.scheme_code, h.nav_date DESC
            ),
            nav_30d AS (
                SELECT DISTINCT ON (scheme_code)
                    scheme_code,
                    nav
                FROM mf_nav_history
                WHERE nav_date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY scheme_code, nav_date DESC
            ),
            nav_1y AS (
                SELECT DISTINCT ON (scheme_code)
                    scheme_code,
                    nav
                FROM mf_nav_history
                WHERE nav_date <= CURRENT_DATE - INTERVAL '1 year'
                ORDER BY scheme_code, nav_date DESC
            ),
            start_nav AS (
                SELECT DISTINCT ON (scheme_code)
                    scheme_code,
                    nav
                FROM mf_nav_history
                ORDER BY scheme_code, nav_date ASC
            ),
            sparkline AS (
                SELECT
                    scheme_code,
                    jsonb_agg(nav ORDER BY nav_date) AS navs
                FROM (
                    SELECT
                        scheme_code,
                        nav_date,
                        nav
                    FROM mf_nav_history
                    WHERE nav_date >= CURRENT_DATE - INTERVAL '1 year'
                ) t
                GROUP BY scheme_code
            )
            SELECT
                s.scheme_code,
                s.scheme_name,
                s.scheme_category,
                s.start_date,
                l.nav AS latest_nav,
                l.nav_date AS latest_nav_date,
                p.nav AS prev_nav,
                n30.nav AS nav_30d,
                n1y.nav AS nav_1y,
                sn.nav AS start_nav,
                sp.navs
            FROM mf_schemes s
            JOIN latest_nav l ON l.scheme_code = s.scheme_code
            LEFT JOIN prev_nav p ON p.scheme_code = s.scheme_code
            LEFT JOIN nav_30d n30 ON n30.scheme_code = s.scheme_code
            LEFT JOIN nav_1y n1y ON n1y.scheme_code = s.scheme_code
            LEFT JOIN start_nav sn ON sn.scheme_code = s.scheme_code
            LEFT JOIN sparkline sp ON sp.scheme_code = s.scheme_code;
            """
        )
        return cur.fetchall()


def build_category_structure(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    subcat_counts: Dict[str, int] = {}
    for row in rows:
        category = str(row.get("category") or "")
        if category:
            subcat_counts[category] = subcat_counts.get(category, 0) + 1

    structure: Dict[str, Dict[str, Any]] = {}
    for primary, subcategories in CATEGORY_STRUCTURE.items():
        primary_count = 0
        subcat_list = []

        for subcat in subcategories:
            count = 0
            for cat_name, cat_count in subcat_counts.items():
                if subcat.lower() in cat_name.lower():
                    count += cat_count

            if count > 0:
                subcat_list.append({"name": subcat, "count": count})
                primary_count += count

        if primary_count > 0:
            structure[primary] = {
                "count": primary_count,
                "subcategories": subcat_list,
            }

    return structure


def generate_snapshot(conn) -> None:
    print("Generating mutual fund snapshot...")
    rows = fetch_base_rows(conn)
    print(f"Processing {len(rows)} schemes...")

    generated_at = utc_now().isoformat()
    as_of_dates: List[date] = []
    dataset: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        for row in rows:
            (
                scheme_code,
                scheme_name,
                category,
                start_date,
                latest_nav,
                latest_nav_date,
                prev_nav,
                nav_30d,
                nav_1y,
                start_nav,
                sparkline,
            ) = row

            if _is_etf_scheme(str(scheme_name or "")):
                continue

            age_years = None
            if start_date:
                age_years = (datetime.utcnow().date() - start_date).days / 365

            return_1d = pct_change(latest_nav, prev_nav)
            return_30d = pct_change(latest_nav, nav_30d)
            return_1y = pct_change(latest_nav, nav_1y)
            since_inception_return = pct_change(latest_nav, start_nav)

            compressed_sparkline: List[float] = []
            if sparkline:
                if isinstance(sparkline, str):
                    try:
                        sparkline = json.loads(sparkline)
                    except Exception:
                        sparkline = []
                step = max(1, len(sparkline) // 52)
                compressed_sparkline = [float(v) for v in sparkline[::step] if v is not None]

            cur.execute(
                """
                INSERT INTO mf_performance_snapshot (
                    scheme_code,
                    scheme_name,
                    category,
                    latest_nav,
                    age_years,
                    return_1d,
                    return_30d,
                    return_1y,
                    since_inception_return,
                    sparkline_1y,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (scheme_code)
                DO UPDATE SET
                    scheme_name = EXCLUDED.scheme_name,
                    category = EXCLUDED.category,
                    latest_nav = EXCLUDED.latest_nav,
                    age_years = EXCLUDED.age_years,
                    return_1d = EXCLUDED.return_1d,
                    return_30d = EXCLUDED.return_30d,
                    return_1y = EXCLUDED.return_1y,
                    since_inception_return = EXCLUDED.since_inception_return,
                    sparkline_1y = EXCLUDED.sparkline_1y,
                    updated_at = NOW();
                """,
                (
                    int(scheme_code),
                    str(scheme_name) if scheme_name is not None else None,
                    str(category) if category is not None else None,
                    latest_nav,
                    age_years,
                    return_1d,
                    return_30d,
                    return_1y,
                    since_inception_return,
                    Json(compressed_sparkline),
                ),
            )

            dataset.append(
                {
                    "scheme_code": int(scheme_code),
                    "scheme_name": str(scheme_name) if scheme_name is not None else "",
                    "category": str(category) if category is not None else None,
                    "latest_nav": _safe_float(latest_nav),
                    "age_years": _safe_float(age_years),
                    "return_1d": _safe_float(return_1d),
                    "return_30d": _safe_float(return_30d),
                    "return_1y": _safe_float(return_1y),
                    "since_inception_return": _safe_float(since_inception_return),
                    "sparkline_1y": compressed_sparkline,
                }
            )

            if latest_nav_date:
                as_of_dates.append(latest_nav_date)

    categories_counts: Dict[str, int] = {}
    fund_house_counts: Dict[str, int] = {}
    for item in dataset:
        cat = str(item.get("category") or "Unknown")
        categories_counts[cat] = categories_counts.get(cat, 0) + 1

        fund_house = _extract_fund_house(item.get("scheme_name", ""))
        fund_house_counts[fund_house] = fund_house_counts.get(fund_house, 0) + 1

    categories = [
        {"category": cat, "count": count}
        for cat, count in sorted(categories_counts.items(), key=lambda x: x[0])
    ]
    fund_houses = [
        {"fund_house": fh, "count": count}
        for fh, count in sorted(fund_house_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    category_structure = build_category_structure(dataset)
    as_of_date = max(as_of_dates) if as_of_dates else None

    upsert_landing_snapshot(
        conn,
        "mutualfundstable:dataset",
        {
            "generated_at": generated_at,
            "items": dataset,
            "total": len(dataset),
        },
        as_of_date,
    )
    upsert_landing_snapshot(
        conn,
        "mutualfundstable:categories",
        {
            "generated_at": generated_at,
            "categories": categories,
        },
        as_of_date,
    )
    upsert_landing_snapshot(
        conn,
        "mutualfundstable:fundhouses",
        {
            "generated_at": generated_at,
            "fund_houses": fund_houses,
        },
        as_of_date,
    )
    upsert_landing_snapshot(
        conn,
        "mutualfundstable:category_structure",
        {
            "generated_at": generated_at,
            "structure": category_structure,
        },
        as_of_date,
    )

    conn.commit()
    print("Mutual fund snapshot tables and landing snapshots updated successfully.")


def main() -> None:
    conn = get_connection()
    try:
        create_tables(conn)
        generate_snapshot(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
