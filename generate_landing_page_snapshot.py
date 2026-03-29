#!/usr/bin/env python3
"""
Generate landing-page snapshots for request-heavy routers and persist them in
`landing_page_snapshots` so API routes can read precomputed payloads.

Scope covered:
- landingpage/stockstable.py
- landingpage/etfstable.py
- landingpage/sectortechnicals.py
- landingpage/sector_advance_decline.py
- landingpage/fii_dii.py
- landingpage/treemap.py
- landingpage/expiry_calendar.py
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError

DEFAULT_DATABASE_URL = (
    "postgresql://postgres:zKvzg4nrnigizkpbc1fN@"
    "budhi.cjwye68sytlr.ap-south-1.rds.amazonaws.com:5432/postgres"
)
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

# Query timeout in seconds (default 5 minutes per query)
QUERY_TIMEOUT_SECONDS = int(os.getenv("QUERY_TIMEOUT_SECONDS", "300"))
SNAPSHOT_JOB_LOCK_KEY = int(os.getenv("SNAPSHOT_JOB_LOCK_KEY", "90210417"))
UPSERT_RETRY_ATTEMPTS = int(os.getenv("SNAPSHOT_UPSERT_RETRY_ATTEMPTS", "5"))
UPSERT_RETRY_BASE_SECONDS = float(os.getenv("SNAPSHOT_UPSERT_RETRY_BASE_SECONDS", "0.25"))

# Use relative path from script location
SCRIPT_DIR = Path(__file__).resolve().parent  # /home/ec2-user
BASE_DIR = SCRIPT_DIR                        # <-- FIX
INDEX_LISTS_DIR = BASE_DIR / "data" / "index_lists"
NIFTY500_FILE = INDEX_LISTS_DIR / "nifty500.txt"
ETF_LIST_FILE = SCRIPT_DIR / "data-ETF-list.csv"

TREEMAP_INDEX_FILE_MAP = {
    "nifty50": "nifty50.txt",
    "nifty100": "nifty100.txt",
    "niftynext50": "niftynext50.txt",
    "nifty500": "nifty500.txt",
    "nifty_bank": "nifty_bank.txt",
    "nifty_it": "nifty_it.txt",
    "nifty_energy": "nifty_energy.txt",
    "nifty_fmcg": "nifty_fmcg.txt",
    "nifty_financial_services": "nifty_financial_services.txt",
    "nifty_auto": "nifty_auto.txt",
    "nifty_infrastructure": "nifty_infrastructure.txt",
    "nifty_oil_gas": "nifty_oil_gas.txt",
    "nifty_pharma": "nifty_pharma.txt",
    "nifty_metal": "nifty_metal.txt",
    "nifty_healthcare": "nifty_healthcare.txt",
    "nifty_consumer_durables": "nifty_consumer_durables.txt",
    "nifty_india_tourism": "nifty_india_tourism.txt",
    "nifty_capital_markets": "nifty_capital_markets.txt",
    "nifty_realty": "nifty_realty.txt",
    "nifty_india_defence": "nifty_india_defence.txt",
    "nifty_media": "nifty_media.txt",
    "nifty_commodities": "nifty_commodities.txt",
    "nifty_psu_bank": "nifty_psu_bank.txt",
}

TREEMAP_EXPECTED_INDEX_KEYS = [
    "nifty50",
    "nifty100",
    "niftynext50",
    "nifty500",
    "nifty_bank",
    "nifty_it",
    "nifty_energy",
    "nifty_fmcg",
    "nifty_financial_services",
    "nifty_auto",
    "nifty_infrastructure",
    "nifty_oil_gas",
    "nifty_pharma",
    "nifty_metal",
    "nifty_healthcare",
    "nifty_consumer_durables",
    "nifty_india_tourism",
    "nifty_capital_markets",
    "nifty_realty",
    "nifty_india_defence",
    "nifty_media",
    "nifty_commodities",
    "nifty_psu_bank",
]

MARKET_DASHBOARD_ALLOWED_INDICES = [
    "GIFT NIFTY",
    "INDIA VIX",
    "SENSEX",
    "NIFTY 50",
    "NIFTY 500",
    "NIFTY MIDCAP 100",
    "NIFTY SMLCAP 250",
    "NIFTY BANK",
    "NIFTY IT",
    "NIFTY ENERGY",
    "NIFTY FMCG",
    "NIFTY FINSEREXBNK",
    "NIFTY AUTO",
    "NIFTY INFRA",
    "NIFTY OIL AND GAS",
    "NIFTY PHARMA",
    "NIFTY METAL",
    "NIFTY HEALTHCARE",
    "NIFTY CONSR DURBL",
    "NIFTY IND TOURISM",
    "NIFTY CAPITAL MKT",
    "NIFTY REALTY",
    "NIFTY IND DEFENCE",
    "NIFTY MEDIA",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pct_change(current: Optional[float], base: Optional[float], digits: int = 2) -> Optional[float]:
    if current is None or base is None or base == 0:
        return None
    return round(((current - base) / base) * 100, digits)


def json_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def create_snapshot_table(conn: Connection) -> None:
    conn.execute(
        text(
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
    )
    conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_landing_page_snapshots_as_of
                ON landing_page_snapshots (as_of_date);
            """
        )
    )


def try_acquire_snapshot_job_lock(conn: Connection) -> bool:
    return bool(
        conn.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"),
            {"lock_key": SNAPSHOT_JOB_LOCK_KEY},
        ).scalar()
    )


def release_snapshot_job_lock(conn: Connection) -> None:
    conn.execute(
        text("SELECT pg_advisory_unlock(:lock_key)"),
        {"lock_key": SNAPSHOT_JOB_LOCK_KEY},
    )


def _is_retryable_snapshot_error(exc: OperationalError) -> bool:
    code = getattr(getattr(exc, "orig", None), "pgcode", None)
    return code in {"40P01", "40001"}


def upsert_snapshot(conn: Connection, key: str, payload: Dict[str, Any], as_of_date: Optional[date] = None) -> None:
    delete_statement = text(
        """
        DELETE FROM landing_page_snapshots
        WHERE snapshot_key = :snapshot_key;
        """
    )
    insert_statement = text(
        """
        INSERT INTO landing_page_snapshots (
            snapshot_key,
            snapshot_data,
            as_of_date,
            generated_at,
            updated_at
        )
        VALUES (
            :snapshot_key,
            CAST(:snapshot_data AS JSONB),
            :as_of_date,
            NOW(),
            NOW()
        );
        """
    )
    params = {
        "snapshot_key": key,
        "snapshot_data": json.dumps(payload, default=json_default),
        "as_of_date": as_of_date,
    }

    for attempt in range(UPSERT_RETRY_ATTEMPTS):
        try:
            conn.execute(delete_statement, {"snapshot_key": key})
            conn.execute(insert_statement, params)
            return
        except OperationalError as exc:
            if not _is_retryable_snapshot_error(exc) or attempt == UPSERT_RETRY_ATTEMPTS - 1:
                raise
            sleep_seconds = UPSERT_RETRY_BASE_SECONDS * (2 ** attempt)
            print(
                f"⚠️ Retryable DB error while upserting {key}. "
                f"Retry {attempt + 1}/{UPSERT_RETRY_ATTEMPTS} in {sleep_seconds:.2f}s"
            )
            time.sleep(sleep_seconds)


def _read_existing_snapshot(conn: Connection, key: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        text(
            """
            SELECT snapshot_data
            FROM landing_page_snapshots
            WHERE snapshot_key = :snapshot_key
            LIMIT 1
            """
        ),
        {"snapshot_key": key},
    ).mappings().first()
    if not row:
        return None
    snapshot_data = row.get("snapshot_data")
    if isinstance(snapshot_data, dict):
        return snapshot_data
    if isinstance(snapshot_data, str):
        try:
            parsed = json.loads(snapshot_data)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _is_effectively_empty_snapshot(key: str, payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(payload, dict):
        return True

    if key in {"stockstable:dataset", "etfstable:dataset"}:
        return len(payload.get("items") or []) == 0
    if key in {"stockstable:sectors", "etfstable:sectors"}:
        return len(payload.get("sectors") or []) == 0
    if key == "marketdashboard:instruments":
        return len(payload.get("instruments") or []) == 0
    if key.startswith("marketdashboard:symbol:"):
        return len(payload.get("current_data") or []) == 0
    if key == "treemap:companies":
        return len(payload.get("items") or []) == 0
    if key.startswith("treemap:"):
        if key in {"treemap:data_status"}:
            return False
        return len(payload.get("nodes") or []) == 0

    return False


def upsert_snapshot_preserve_last_non_empty(
    conn: Connection,
    key: str,
    payload: Dict[str, Any],
    as_of_date: Optional[date] = None,
) -> None:
    if not _is_effectively_empty_snapshot(key, payload):
        upsert_snapshot(conn, key, payload, as_of_date=as_of_date)
        return

    existing_payload = _read_existing_snapshot(conn, key)
    if existing_payload and not _is_effectively_empty_snapshot(key, existing_payload):
        print(f"⚠️ Preserving last non-empty snapshot for {key} (new payload was empty)")
        return

    upsert_snapshot(conn, key, payload, as_of_date=as_of_date)


def fetch_stockstable_raw(conn: Connection) -> List[Dict[str, Any]]:
    query = text(
        """
        WITH latest_60 AS (
            SELECT
                tradingsymbol,
                ts,
                close,
                volume,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '60minute'
              AND ts >= (SELECT MAX(DATE(ts)) - INTERVAL '7 days' FROM ohlcv WHERE interval = '1day')
        ),
        agg_60 AS (
            SELECT
                tradingsymbol,
                MAX(CASE WHEN rn = 1 THEN close END) AS ltp,
                MAX(CASE WHEN rn = 2 THEN close END) AS prev_1h_close
            FROM latest_60
            GROUP BY tradingsymbol
        ),
        latest_day AS (
            SELECT
                tradingsymbol,
                ts,
                close,
                volume,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '1day'
              AND ts >= (SELECT MAX(DATE(ts)) - INTERVAL '14 days' FROM ohlcv WHERE interval = '1day')
        ),
        agg_day AS (
            SELECT
                tradingsymbol,
                MAX(CASE WHEN rn = 1 THEN volume END) AS volume_1d,
                MAX(CASE WHEN rn = 2 THEN close END) AS close_1d,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_6d
            FROM latest_day
            GROUP BY tradingsymbol
        ),
        spark AS (
            SELECT
                tradingsymbol,
                ARRAY_AGG(close ORDER BY ts) AS sparkline_7d
            FROM latest_day
            WHERE rn <= 6
            GROUP BY tradingsymbol
        ),
        latest_52w AS (
            SELECT
                tradingsymbol,
                COALESCE(high, close) AS high,
                COALESCE(low, close) AS low,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '1day'
              AND ts >= (SELECT MAX(DATE(ts)) - INTERVAL '420 days' FROM ohlcv WHERE interval = '1day')
        ),
        agg_52w AS (
            SELECT
                tradingsymbol,
                MAX(high) FILTER (WHERE rn <= 252) AS high_52w,
                MIN(low) FILTER (WHERE rn <= 252) AS low_52w
            FROM latest_52w
            GROUP BY tradingsymbol
        )
        SELECT
            i.tradingsymbol AS symbol,
            COALESCE(NULLIF(i.name, ''), i.tradingsymbol) AS name,
            i.sector AS sector,
            i.total_shares_held AS total_shares_held,
            a60.ltp AS ltp,
            a60.prev_1h_close AS prev_1h_close,
            ad.volume_1d AS volume_24h,
            ad.close_1d AS close_1d,
            ad.close_6d AS close_6d,
            sp.sparkline_7d AS sparkline_7d,
            a52.high_52w AS high_52w,
            a52.low_52w AS low_52w
        FROM instruments i
        LEFT JOIN agg_60 a60 ON a60.tradingsymbol = i.tradingsymbol
        LEFT JOIN agg_day ad ON ad.tradingsymbol = i.tradingsymbol
        LEFT JOIN spark sp ON sp.tradingsymbol = i.tradingsymbol
        LEFT JOIN agg_52w a52 ON a52.tradingsymbol = i.tradingsymbol
        WHERE i.segment = 'NSE'
        ORDER BY i.tradingsymbol
        """
    )
    rows = conn.execute(query).mappings().all()
    return [dict(row) for row in rows]


def fetch_marketdashboard_instruments(conn: Connection, symbols: List[str]) -> List[Dict[str, Any]]:
    query = text(
        """
        SELECT
            tradingsymbol AS symbol,
            name,
            segment,
            exchange,
            instrument_token AS token
        FROM instruments
        WHERE segment = 'INDICES'
          AND tradingsymbol = ANY(:symbols)
        ORDER BY tradingsymbol
        """
    )
    rows = conn.execute(query, {"symbols": symbols}).mappings().all()
    return [dict(row) for row in rows]


def fetch_marketdashboard_ohlcv_rows(conn: Connection, symbols: List[str]) -> List[Dict[str, Any]]:
    query = text(
        """
        WITH filtered AS (
            SELECT
                tradingsymbol,
                ts,
                open,
                high,
                low,
                close,
                volume,
                DATE(ts + INTERVAL '5 hours 30 minutes') AS trade_date
            FROM ohlcv
            WHERE interval = '15minute'
              AND tradingsymbol = ANY(:symbols)
              AND ts >= (SELECT MAX(DATE(ts)) - INTERVAL '21 days' FROM ohlcv WHERE interval = '15minute' AND tradingsymbol = ANY(:symbols))
              AND (
                    EXTRACT(HOUR FROM (ts + INTERVAL '5 hours 30 minutes')) * 60
                    + EXTRACT(MINUTE FROM (ts + INTERVAL '5 hours 30 minutes'))
                  ) BETWEEN (9 * 60 + 15) AND (15 * 60 + 30)
        ),
        latest_dates AS (
            SELECT tradingsymbol, MAX(trade_date) AS latest_trade_date
            FROM filtered
            GROUP BY tradingsymbol
        )
        SELECT
            f.tradingsymbol,
            f.ts,
            f.open,
            f.high,
            f.low,
            f.close,
            f.volume,
            f.trade_date
        FROM filtered f
        INNER JOIN latest_dates ld
            ON ld.tradingsymbol = f.tradingsymbol
           AND ld.latest_trade_date = f.trade_date
        ORDER BY f.tradingsymbol, f.ts
        """
    )
    rows = conn.execute(query, {"symbols": symbols}).mappings().all()
    return [dict(row) for row in rows]


def fetch_marketdashboard_previous_close(conn: Connection, symbols: List[str]) -> Dict[str, Optional[float]]:
    query = text(
        """
        WITH ranked AS (
            SELECT
                tradingsymbol,
                close,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '1day'
              AND tradingsymbol = ANY(:symbols)
        )
        SELECT tradingsymbol, close
        FROM ranked
        WHERE rn = 2
        """
    )
    rows = conn.execute(query, {"symbols": symbols}).mappings().all()
    payload: Dict[str, Optional[float]] = {symbol: None for symbol in symbols}
    for row in rows:
        symbol = str(row.get("tradingsymbol") or "")
        payload[symbol] = safe_float(row.get("close"))
    return payload


def build_marketdashboard_symbol_snapshots(
    symbols: List[str],
    ohlcv_rows: List[Dict[str, Any]],
    previous_close_map: Dict[str, Optional[float]],
) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    as_of_date_by_symbol: Dict[str, Optional[date]] = {symbol: None for symbol in symbols}

    for row in ohlcv_rows:
        symbol = str(row.get("tradingsymbol") or "")
        if not symbol:
            continue
        grouped[symbol].append(
            {
                "date": row.get("ts").isoformat() if row.get("ts") else None,
                "open": safe_float(row.get("open")) or 0.0,
                "high": safe_float(row.get("high")) or 0.0,
                "low": safe_float(row.get("low")) or 0.0,
                "close": safe_float(row.get("close")) or 0.0,
                "volume": int(row.get("volume") or 0),
            }
        )
        trade_date = row.get("trade_date")
        if isinstance(trade_date, date):
            as_of_date_by_symbol[symbol] = trade_date

    generated_at = utc_now().isoformat()
    snapshots: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        snapshots[symbol] = {
            "symbol": symbol,
            "as_of_date": as_of_date_by_symbol[symbol].isoformat() if as_of_date_by_symbol[symbol] else None,
            "generated_at": generated_at,
            "previous_close": previous_close_map.get(symbol),
            "current_data": grouped.get(symbol, []),
        }
    return snapshots


def load_etf_symbols(etf_csv: Path) -> List[str]:
    if not etf_csv.exists():
        raise FileNotFoundError(f"ETF list file missing: {etf_csv}")

    symbols: List[str] = []
    with etf_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "tradingsymbol" in (reader.fieldnames or []):
            for row in reader:
                sym = str(row.get("tradingsymbol") or "").strip().upper()
                if sym:
                    symbols.append(sym)
        else:
            handle.seek(0)
            for line in handle:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if parts and parts[0].lower() != "instrument_token" and len(parts) > 1:
                    sym = parts[1].upper()
                    if sym:
                        symbols.append(sym)

    return sorted(set(symbols))


def build_stockstable_snapshot(raw_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    dataset: List[Dict[str, Any]] = []
    sector_counts: Dict[str, int] = {}

    for row in raw_rows:
        ltp = safe_float(row.get("ltp"))
        prev_1h_close = safe_float(row.get("prev_1h_close"))
        close_1d = safe_float(row.get("close_1d"))
        close_6d = safe_float(row.get("close_6d"))
        total_shares_held = safe_float(row.get("total_shares_held"))
        price_for_market_cap = ltp if ltp is not None else close_1d
        market_cap = (
            round(price_for_market_cap * total_shares_held, 2)
            if price_for_market_cap is not None and total_shares_held is not None
            else None
        )
        sparkline = [safe_float(v) for v in (row.get("sparkline_7d") or [])]
        sparkline = [v for v in sparkline if v is not None]
        sector = str(row.get("sector") or "Unknown")
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

        dataset.append(
            {
                "symbol": str(row.get("symbol") or ""),
                "name": str(row.get("name") or row.get("symbol") or ""),
                "sector": row.get("sector"),
                "ltp": ltp,
                "change_1h_pct": pct_change(ltp, prev_1h_close, digits=4),
                "change_24h_pct": pct_change(ltp, close_1d, digits=4),
                "change_7d_pct": pct_change(ltp, close_6d, digits=4),
                "market_cap": market_cap,
                "volume_24h": safe_float(row.get("volume_24h")),
                "high_52w": safe_float(row.get("high_52w")),
                "low_52w": safe_float(row.get("low_52w")),
                "sparkline_7d": sparkline,
                "data_source": "database_snapshot",
            }
        )

    sectors = [{"sector": sec, "count": count} for sec, count in sorted(sector_counts.items(), key=lambda x: x[0])]
    return {
        "generated_at": utc_now().isoformat(),
        "items": dataset,
        "total": len(dataset),
        "sectors": sectors,
    }


def fetch_sectortech_symbol_snapshot(conn: Connection) -> Tuple[date, List[Dict[str, Any]]]:
    query = text(
        """
        WITH latest_date AS (
            SELECT MAX(DATE(ts)) AS as_of_date
            FROM ohlcv
            WHERE interval = '1day'
            LIMIT 1
        ),
        sector_symbols AS (
            SELECT DISTINCT
                i.tradingsymbol AS symbol,
                COALESCE(si.index_name, 'Other') AS index_name,
                COALESCE(NULLIF(i.name, ''), i.tradingsymbol) AS company_name
            FROM instruments i
            LEFT JOIN sector_indices si ON i.tradingsymbol = si.symbol
            WHERE i.segment = 'NSE'
        ),
        ranked_data AS (
            SELECT
                o.tradingsymbol,
                o.close,
                o.high,
                o.low,
                DATE(o.ts) AS trade_date,
                ROW_NUMBER() OVER (PARTITION BY o.tradingsymbol ORDER BY o.ts DESC) AS rn
            FROM ohlcv o
            INNER JOIN sector_symbols ss ON o.tradingsymbol = ss.symbol
            WHERE o.interval = '1day'
              AND o.ts >= (SELECT MAX(DATE(ts)) - INTERVAL '380 days' FROM ohlcv WHERE interval = '1day')
        ),
        aggregated AS (
            SELECT
                tradingsymbol,
                MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
                MAX(CASE WHEN rn = 2 THEN close END) AS prev_close,
                MAX(CASE WHEN rn = 1 THEN high END) AS latest_high,
                MIN(CASE WHEN rn = 1 THEN low END) AS latest_low,
                MAX(CASE WHEN rn = 1 THEN trade_date END) AS latest_date,
                MAX(high) FILTER (WHERE rn <= 252) AS high_52w,
                MIN(low) FILTER (WHERE rn <= 252) AS low_52w,
                MAX(high) FILTER (WHERE rn >= 2 AND rn <= 253) AS high_52w_prev,
                MIN(low) FILTER (WHERE rn >= 2 AND rn <= 253) AS low_52w_prev,
                AVG(CASE WHEN rn <= 50 THEN close END) AS sma50,
                AVG(CASE WHEN rn >= 2 AND rn <= 51 THEN close END) AS sma50_prev,
                AVG(CASE WHEN rn <= 200 THEN close END) AS sma200,
                AVG(CASE WHEN rn >= 2 AND rn <= 201 THEN close END) AS sma200_prev,
                COUNT(*) AS rows_available
            FROM ranked_data
            GROUP BY tradingsymbol
        )
        SELECT
            ld.as_of_date,
            ss.symbol AS symbol,
            COALESCE(NULLIF(ss.company_name, ''), ss.symbol) AS name,
            ss.index_name AS sector,
            a.latest_close,
            a.prev_close,
            a.latest_high,
            a.latest_low,
            a.high_52w,
            a.low_52w,
            a.high_52w_prev,
            a.low_52w_prev,
            a.sma50,
            a.sma50_prev,
            a.sma200,
            a.sma200_prev,
            COALESCE(a.rows_available, 0) AS rows_available,
            CASE WHEN a.latest_date = ld.as_of_date THEN 1 ELSE 0 END AS has_as_of
        FROM sector_symbols ss
        CROSS JOIN latest_date ld
        LEFT JOIN aggregated a ON a.tradingsymbol = ss.symbol
        """
    )
    rows = conn.execute(query).mappings().all()
    if not rows:
        raise RuntimeError("No sector technical rows available")

    as_of_date = rows[0]["as_of_date"]
    if as_of_date is None:
        raise RuntimeError("Latest sector technical date not found")

    parsed_rows: List[Dict[str, Any]] = []
    for row in rows:
        parsed_rows.append(
            {
                "symbol": str(row["symbol"]),
                "name": str(row["name"]),
                "sector": str(row["sector"]),
                "latest_close": safe_float(row["latest_close"]),
                "prev_close": safe_float(row["prev_close"]),
                "latest_high": safe_float(row["latest_high"]),
                "latest_low": safe_float(row["latest_low"]),
                "high_52w": safe_float(row["high_52w"]),
                "low_52w": safe_float(row["low_52w"]),
                "high_52w_prev": safe_float(row["high_52w_prev"]),
                "low_52w_prev": safe_float(row["low_52w_prev"]),
                "sma50": safe_float(row["sma50"]),
                "sma50_prev": safe_float(row["sma50_prev"]),
                "sma200": safe_float(row["sma200"]),
                "sma200_prev": safe_float(row["sma200_prev"]),
                "rows_available": int(row["rows_available"] or 0),
                "has_as_of": bool(row["has_as_of"]),
            }
        )
    return as_of_date, parsed_rows


def build_sectortech_advance_decline(as_of_date: date, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "total": 0,
            "advances_stocks": [],
            "declines_stocks": [],
            "unchanged_stocks": [],
        }
    )

    for row in rows:
        latest = row["latest_close"]
        prev = row["prev_close"]
        if not row["has_as_of"] or latest is None or prev is None:
            continue

        sector = row["sector"]
        grouped[sector]["total"] += 1
        info = {
            "symbol": row["symbol"],
            "name": row["name"],
            "price": round(latest, 2),
            "pct_change": pct_change(latest, prev, digits=2),
        }

        if latest > prev:
            grouped[sector]["advances"] += 1
            grouped[sector]["advances_stocks"].append(info)
        elif latest < prev:
            grouped[sector]["declines"] += 1
            grouped[sector]["declines_stocks"].append(info)
        else:
            grouped[sector]["unchanged"] += 1
            grouped[sector]["unchanged_stocks"].append(info)

    sectors: List[Dict[str, Any]] = []
    for sector in sorted(grouped.keys()):
        data = grouped[sector]
        total = data["total"]
        if total > 0:
            advances_pct = round((data["advances"] / total) * 100, 1)
            declines_pct = round((data["declines"] / total) * 100, 1)
            unchanged_pct = round((data["unchanged"] / total) * 100, 1)
        else:
            advances_pct = declines_pct = unchanged_pct = 0.0

        sectors.append(
            {
                "sector": sector,
                "advances": data["advances"],
                "declines": data["declines"],
                "unchanged": data["unchanged"],
                "total": total,
                "advances_pct": advances_pct,
                "declines_pct": declines_pct,
                "unchanged_pct": unchanged_pct,
                "advances_stocks": data["advances_stocks"],
                "declines_stocks": data["declines_stocks"],
                "unchanged_stocks": data["unchanged_stocks"],
            }
        )

    return {
        "as_of_date": as_of_date.isoformat(),
        "generated_at": utc_now().isoformat(),
        "sectors": sectors,
    }


def build_sectortech_breadth(as_of_date: date, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Snapshot source may include duplicate symbol rows (same symbol across multiple index memberships).
    # Breadth metrics must be symbol-level unique.
    unique_rows_by_symbol: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        if symbol not in unique_rows_by_symbol:
            unique_rows_by_symbol[symbol] = row

    normalized_rows = list(unique_rows_by_symbol.values())
    total_symbols = len(normalized_rows)
    as_of_rows = [r for r in normalized_rows if r["has_as_of"] and r["latest_close"] is not None]

    def stocks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        payload = []
        for item in items:
            latest = item["latest_close"]
            prev = item["prev_close"]
            payload.append(
                {
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "price": round(latest, 2) if latest is not None else None,
                    "pct_change": pct_change(latest, prev, digits=2) if prev is not None else None,
                }
            )
        return payload

    def metric(eligible: List[Dict[str, Any]], above: List[Dict[str, Any]], below: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_counted = len(above) + len(below)
        if total_counted > 0:
            above_pct = round((len(above) / total_counted) * 100, 1)
            below_pct = round((len(below) / total_counted) * 100, 1)
        else:
            above_pct = below_pct = 0.0
        return {
            "as_of_date": as_of_date.isoformat(),
            "total_symbols": total_symbols,
            "eligible_symbols": len(eligible),
            "above_count": len(above),
            "below_count": len(below),
            "above_pct": above_pct,
            "below_pct": below_pct,
            "above_stocks": stocks(above),
            "below_stocks": stocks(below),
        }

    # 52W breakout detection based on today's candle extremes vs current 52W range:
    # High breakout: today's high >= 52W highest high
    # Low breakdown: today's low <= 52W lowest low
    eligible_52w = [
        r for r in as_of_rows
        if r["rows_available"] >= 252
        and r["latest_high"] is not None
        and r["latest_low"] is not None
        and r["high_52w"] is not None
        and r["low_52w"] is not None
    ]
    above_52w = [r for r in eligible_52w if r["latest_high"] >= r["high_52w"]]
    below_52w = [r for r in eligible_52w if r["latest_low"] <= r["low_52w"]]

    # SMA50 Crossover Detection
    # Cross Over: prev_close was below sma50_prev AND latest_close is above sma50
    # Cross Under: prev_close was above sma50_prev AND latest_close is below sma50
    eligible_sma50 = [
        r for r in as_of_rows 
        if r["rows_available"] >= 51 
        and r["sma50"] is not None 
        and r["sma50_prev"] is not None
        and r["prev_close"] is not None
    ]
    crossover_sma50 = [
        r for r in eligible_sma50 
        if r["prev_close"] < r["sma50_prev"] and r["latest_close"] > r["sma50"]
    ]
    crossunder_sma50 = [
        r for r in eligible_sma50 
        if r["prev_close"] > r["sma50_prev"] and r["latest_close"] < r["sma50"]
    ]

    # SMA200 Crossover Detection
    # Cross Over: prev_close was below sma200_prev AND latest_close is above sma200
    # Cross Under: prev_close was above sma200_prev AND latest_close is below sma200
    eligible_sma200 = [
        r for r in as_of_rows
        if r["rows_available"] >= 201
        and r["sma200"] is not None
        and r["sma200_prev"] is not None
        and r["prev_close"] is not None
    ]
    crossover_sma200 = [
        r for r in eligible_sma200
        if r["prev_close"] < r["sma200_prev"] and r["latest_close"] > r["sma200"]
    ]
    crossunder_sma200 = [
        r for r in eligible_sma200
        if r["prev_close"] > r["sma200_prev"] and r["latest_close"] < r["sma200"]
    ]

    return {
        "generated_at": utc_now().isoformat(),
        "fifty_two_week": metric(eligible_52w, above_52w, below_52w),
        "sma50": metric(eligible_sma50, crossover_sma50, crossunder_sma50),
        "sma200": metric(eligible_sma200, crossover_sma200, crossunder_sma200),
    }


def load_symbols_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise RuntimeError(f"Missing symbols file: {path}")
    return [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def fetch_sectortech_top_movers(conn: Connection, symbols: List[str]) -> List[Dict[str, Any]]:
    query = text(
        """
        WITH ranked_data AS (
            SELECT
                tradingsymbol,
                ts,
                close,
                volume,
                DATE(ts) AS trade_date,
                LAG(close) OVER (PARTITION BY tradingsymbol ORDER BY ts) AS prev_close,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol, DATE(ts) ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '1day'
              AND tradingsymbol = ANY(:symbols)
              AND ts >= (SELECT MAX(DATE(ts)) - INTERVAL '5 days' FROM ohlcv WHERE interval = '1day' AND tradingsymbol = ANY(:symbols))
        ),
        latest_date AS (
            SELECT MAX(trade_date) AS as_of_date
            FROM ranked_data
        ),
        latest_data AS (
            SELECT
                rd.tradingsymbol,
                rd.close AS last,
                rd.volume,
                rd.prev_close,
                ld.as_of_date
            FROM ranked_data rd
            CROSS JOIN latest_date ld
            WHERE rd.trade_date = ld.as_of_date
              AND rd.rn = 1
        )
        SELECT
            ld.as_of_date,
            ld.tradingsymbol AS symbol,
            COALESCE(NULLIF(i.name, ''), ld.tradingsymbol) AS name,
            ld.last,
            ld.volume,
            ld.prev_close
        FROM latest_data ld
        LEFT JOIN instruments i ON i.tradingsymbol = ld.tradingsymbol
        WHERE ld.prev_close IS NOT NULL
        """
    )
    rows = conn.execute(query, {"symbols": symbols}).mappings().all()
    return [dict(row) for row in rows]


def build_sectortech_top_movers(rows: List[Dict[str, Any]], limit: int = 100) -> Dict[str, Any]:
    if not rows:
        return {"as_of_date": None, "generated_at": utc_now().isoformat(), "top_gainers": [], "top_losers": []}

    as_of = rows[0]["as_of_date"]
    movers: List[Dict[str, Any]] = []
    for row in rows:
        last = safe_float(row.get("last"))
        prev = safe_float(row.get("prev_close"))
        change = pct_change(last, prev, digits=2) if last is not None and prev is not None else None
        if change is None:
            continue
        movers.append(
            {
                "symbol": str(row["symbol"]),
                "name": str(row["name"]),
                "last": last,
                "pct_change": change,
                "volume": safe_float(row.get("volume")),
            }
        )

    top_gainers = sorted(movers, key=lambda x: x["pct_change"], reverse=True)[:limit]
    top_losers = sorted(movers, key=lambda x: x["pct_change"])[:limit]
    return {
        "as_of_date": as_of.isoformat() if as_of else None,
        "generated_at": utc_now().isoformat(),
        "top_gainers": [{**item, "signal": "Top Gainers"} for item in top_gainers],
        "top_losers": [{**item, "signal": "Top Losers"} for item in top_losers],
    }


def fetch_sector_indices(conn: Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT
                index_name,
                sector,
                symbol,
                company_name,
                industry,
                series,
                isin_code
            FROM sector_indices
            ORDER BY index_name, symbol
            """
        )
    ).mappings().all()
    return [dict(row) for row in rows]


def fetch_symbol_latest_previous(conn: Connection, symbols: List[str], interval: str, lookback_periods: int) -> Dict[str, Dict[str, Any]]:
    rn_prev = lookback_periods + 1
    query = text(
        """
        WITH ranked AS (
            SELECT
                tradingsymbol,
                close,
                ts,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE tradingsymbol = ANY(:symbols)
              AND interval = :interval
        )
        SELECT
            tradingsymbol,
            MAX(CASE WHEN rn = 1 THEN close END) AS current_close,
            MAX(CASE WHEN rn = :rn_prev THEN close END) AS prev_close,
            MAX(CASE WHEN rn = 1 THEN ts END) AS current_time,
            MAX(CASE WHEN rn = :rn_prev THEN ts END) AS previous_time
        FROM ranked
        GROUP BY tradingsymbol
        """
    )
    rows = conn.execute(query, {"symbols": symbols, "interval": interval, "rn_prev": rn_prev}).mappings().all()
    payload: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        current_close = safe_float(row.get("current_close"))
        prev_close = safe_float(row.get("prev_close"))
        status = None
        if current_close is not None and prev_close is not None:
            if current_close > prev_close:
                status = "advancing"
            elif current_close < prev_close:
                status = "declining"
            else:
                status = "unchanged"

        payload[str(row["tradingsymbol"])] = {
            "current_close": current_close,
            "prev_close": prev_close,
            "current_time": row.get("current_time"),
            "previous_time": row.get("previous_time"),
            "status": status,
            "change_pct": pct_change(current_close, prev_close, digits=2) if status is not None else None,
        }
    return payload


def build_sector_advance_decline_snapshots(
    sector_index_rows: List[Dict[str, Any]],
    symbol_metrics: Dict[str, Dict[str, Any]],
    interval: str,
    lookback_periods: int,
) -> Dict[str, Any]:
    index_to_symbols: Dict[str, List[str]] = defaultdict(list)
    sector_to_symbols: Dict[str, List[str]] = defaultdict(list)
    stocks_by_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    stocks_by_sector: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in sector_index_rows:
        index_name = str(row["index_name"])
        sector_name = str(row["sector"])
        symbol = str(row["symbol"])

        index_to_symbols[index_name].append(symbol)
        sector_to_symbols[sector_name].append(symbol)

        stock_item = {
            "symbol": symbol,
            "company_name": row["company_name"],
            "industry": row["industry"],
            "series": row["series"],
            "isin_code": row["isin_code"],
        }
        stocks_by_index[index_name].append(stock_item)
        stocks_by_sector[sector_name].append(stock_item)

    sector_list = []
    for key, stocks in stocks_by_index.items():
        sector_name = next((r["sector"] for r in sector_index_rows if r["index_name"] == key), None)
        sector_list.append({"index_name": key, "sector": sector_name, "stock_count": len(stocks)})
    sector_list.sort(key=lambda x: x["index_name"])

    def per_group(group_map: Dict[str, List[str]]) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for group_name, symbols in group_map.items():
            rows = []
            for symbol in symbols:
                metric = symbol_metrics.get(symbol)
                if not metric or metric["status"] is None or metric["change_pct"] is None:
                    continue
                rows.append({"tradingsymbol": symbol, **metric})

            advancing = [r for r in rows if r["status"] == "advancing"]
            declining = [r for r in rows if r["status"] == "declining"]
            unchanged = [r for r in rows if r["status"] == "unchanged"]

            top_gainers = sorted(rows, key=lambda x: x["change_pct"], reverse=True)[:5]
            top_losers = sorted(rows, key=lambda x: x["change_pct"])[:5]

            avg_change = round(sum(r["change_pct"] for r in rows) / len(rows), 2) if rows else 0
            results[group_name] = {
                "success": True,
                "sector": group_name,
                "interval": interval,
                "lookback_periods": lookback_periods,
                "total_stocks": len(symbols),
                "data_available": len(rows),
                "advancing": len(advancing),
                "declining": len(declining),
                "unchanged": len(unchanged),
                "advance_decline_ratio": round(len(advancing) / len(declining), 2) if len(declining) > 0 else None,
                "avg_change_pct": avg_change,
                "top_gainers": [
                    {
                        "tradingsymbol": row["tradingsymbol"],
                        "change_pct": row["change_pct"],
                        "current_close": row["current_close"],
                    }
                    for row in top_gainers
                ],
                "top_losers": [
                    {
                        "tradingsymbol": row["tradingsymbol"],
                        "change_pct": row["change_pct"],
                        "current_close": row["current_close"],
                    }
                    for row in top_losers
                ],
                "timestamp": utc_now().isoformat(),
            }
        return results

    all_sectors_summary = []
    for index_name, symbols in index_to_symbols.items():
        rows = []
        for symbol in symbols:
            metric = symbol_metrics.get(symbol)
            if not metric or metric["status"] is None or metric["change_pct"] is None:
                continue
            rows.append(metric)

        advancing = len([r for r in rows if r["status"] == "advancing"])
        declining = len([r for r in rows if r["status"] == "declining"])
        unchanged = len([r for r in rows if r["status"] == "unchanged"])
        avg_change = round(sum(r["change_pct"] for r in rows) / len(rows), 2) if rows else 0

        sector_category = next((r["sector"] for r in sector_index_rows if r["index_name"] == index_name), None)
        all_sectors_summary.append(
            {
                "sector": index_name,
                "sector_category": sector_category,
                "total_stocks": len(symbols),
                "advancing": advancing,
                "declining": declining,
                "unchanged": unchanged,
                "advance_decline_ratio": round(advancing / declining, 2) if declining > 0 else None,
                "avg_change_pct": avg_change,
            }
        )
    all_sectors_summary.sort(key=lambda x: x["sector"])

    return {
        "sector_list": {
            "success": True,
            "count": len(sector_list),
            "sectors": sector_list,
        },
        "sector_stocks": {
            "by_index_name": dict(stocks_by_index),
            "by_sector": dict(stocks_by_sector),
        },
        "advance_decline_by_index": per_group(index_to_symbols),
        "advance_decline_by_sector": per_group(sector_to_symbols),
        "all_sectors_advance_decline": {
            "success": True,
            "interval": interval,
            "sectors_count": len(all_sectors_summary),
            "sectors": all_sectors_summary,
            "timestamp": utc_now().isoformat(),
        },
    }


def fetch_fii_dii_flow(conn: Connection) -> List[Dict[str, Any]]:
    query = text(
        """
        SELECT
            date,
            segment,
            SUM(buy_contracts) AS buy_contracts,
            SUM(buy_value) AS buy_value,
            SUM(sell_contracts) AS sell_contracts,
            SUM(sell_value) AS sell_value,
            SUM(oi_contracts) AS oi_contracts,
            SUM(oi_value) AS oi_value
        FROM fii_derivatives_stats
        WHERE date >= (SELECT MAX(date) - INTERVAL '30 days' FROM fii_derivatives_stats)
        GROUP BY date, segment
        ORDER BY date DESC, segment
        """
    )
    rows = conn.execute(query).mappings().all()
    payload: List[Dict[str, Any]] = []
    for row in rows:
        buy_value = safe_float(row.get("buy_value"))
        sell_value = safe_float(row.get("sell_value"))
        payload.append(
            {
                "date": row.get("date"),
                "segment": row.get("segment"),
                "buy_contracts": row.get("buy_contracts"),
                "buy_value": buy_value,
                "sell_contracts": row.get("sell_contracts"),
                "sell_value": sell_value,
                "net_value": (buy_value - sell_value) if buy_value is not None and sell_value is not None else None,
                "oi_contracts": row.get("oi_contracts"),
                "oi_value": safe_float(row.get("oi_value")),
            }
        )
    return payload


def fetch_participant_oi(conn: Connection) -> List[Dict[str, Any]]:
    query = text(
        """
        SELECT
            date,
            participant_type,
            future_long,
            future_short,
            option_call_long,
            option_call_short,
            option_put_long,
            option_put_short
        FROM fao_participant_oi
        WHERE date >= (SELECT MAX(date) - INTERVAL '30 days' FROM fao_participant_oi)
        ORDER BY date DESC
        """
    )
    rows = conn.execute(query).mappings().all()
    return [dict(row) for row in rows]


def build_fii_dii_flow_snapshot(fii_rows: List[Dict[str, Any]], oi_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not fii_rows:
        return {"records": [], "generated_at": utc_now().isoformat()}

    oi_grouped: Dict[Any, Dict[str, Dict[str, Optional[int]]]] = defaultdict(dict)
    for row in oi_rows:
        trade_date = row.get("date")
        participant_type = row.get("participant_type")
        if trade_date is None or participant_type is None:
            continue
        oi_grouped[trade_date][str(participant_type)] = {
            "future_long": int(row["future_long"]) if not is_null(row.get("future_long")) else None,
            "future_short": int(row["future_short"]) if not is_null(row.get("future_short")) else None,
            "option_call_long": int(row["option_call_long"]) if not is_null(row.get("option_call_long")) else None,
            "option_call_short": int(row["option_call_short"]) if not is_null(row.get("option_call_short")) else None,
            "option_put_long": int(row["option_put_long"]) if not is_null(row.get("option_put_long")) else None,
            "option_put_short": int(row["option_put_short"]) if not is_null(row.get("option_put_short")) else None,
        }

    records = []
    for row in fii_rows:
        date_key = row["date"]
        records.append(
            {
                "date": row["date"].isoformat(),
                "segment": row["segment"],
                "buy_contracts": int(row["buy_contracts"]) if not is_null(row.get("buy_contracts")) else None,
                "buy_value": float(row["buy_value"]) if not is_null(row.get("buy_value")) else None,
                "sell_contracts": int(row["sell_contracts"]) if not is_null(row.get("sell_contracts")) else None,
                "sell_value": float(row["sell_value"]) if not is_null(row.get("sell_value")) else None,
                "net_value": float(row["net_value"]) if not is_null(row.get("net_value")) else None,
                "oi_contracts": int(row["oi_contracts"]) if not is_null(row.get("oi_contracts")) else None,
                "oi_value": float(row["oi_value"]) if not is_null(row.get("oi_value")) else None,
                "participant_oi": oi_grouped.get(date_key, {}),
            }
        )
    return {"records": records, "generated_at": utc_now().isoformat()}


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    txt = str(value).strip().replace(",", "")
    if not txt:
        return None
    try:
        return float(txt)
    except (TypeError, ValueError):
        return None


def _extract_cash_rows_from_nsepython() -> List[Dict[str, Any]]:
    """
    Try fetching cash-market FII/DII from nsepython and normalize to:
    {date, category, buy_value_cr, sell_value_cr, net_value_cr}

    Supports both shapes:
    1) Wide rows: date + fii_buy/fii_sell/fii_net + dii_buy/dii_sell/dii_net
    2) Category rows: date + category + buy/sell/net columns
    """
    try:
        import pandas as pd  # type: ignore
        from nsepython import nse_fiidii  # type: ignore
    except Exception as exc:
        print(f"⚠️ nsepython unavailable for cash-market snapshot: {exc}")
        return []

    try:
        df = nse_fiidii()
    except Exception as exc:
        print(f"⚠️ nse_fiidii fetch failed: {exc}")
        return []

    if df is None or getattr(df, "empty", True):
        print("⚠️ nse_fiidii returned empty dataframe")
        return []

    rows: List[Dict[str, Any]] = []
    col_lookup = {_normalize_col_name(col): col for col in list(df.columns)}

    def pick_col(*candidates: str) -> Optional[str]:
        for candidate in candidates:
            col = col_lookup.get(_normalize_col_name(candidate))
            if col:
                return col
        return None

    date_col = pick_col("date", "trade_date", "tradedate")

    # Shape A: wide rows (fii_*/dii_*)
    fii_buy_col = pick_col("fii_buy", "fiibuy", "fii_buy_value", "fiibuyvalue")
    fii_sell_col = pick_col("fii_sell", "fiisell", "fii_sell_value", "fiisellvalue")
    fii_net_col = pick_col("fii_net", "fiinet", "fii_net_value", "fiinetvalue")
    dii_buy_col = pick_col("dii_buy", "diibuy", "dii_buy_value", "diibuyvalue")
    dii_sell_col = pick_col("dii_sell", "diisell", "dii_sell_value", "diisellvalue")
    dii_net_col = pick_col("dii_net", "diinet", "dii_net_value", "diinetvalue")

    if all([date_col, fii_buy_col, fii_sell_col, fii_net_col, dii_buy_col, dii_sell_col, dii_net_col]):
        frame = df.copy()
        frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce").dt.date
        frame = frame.dropna(subset=[date_col]).sort_values(by=date_col, ascending=False)
        latest_three_dates = list(dict.fromkeys(frame[date_col].tolist()))[:3]
        frame = frame[frame[date_col].isin(latest_three_dates)]

        for _, row in frame.iterrows():
            d = row[date_col]
            date_txt = d.isoformat() if hasattr(d, "isoformat") else str(d)
            rows.append(
                {
                    "date": date_txt,
                    "category": "FII/FPI",
                    "buy_value_cr": _coerce_float(row[fii_buy_col]),
                    "sell_value_cr": _coerce_float(row[fii_sell_col]),
                    "net_value_cr": _coerce_float(row[fii_net_col]),
                }
            )
            rows.append(
                {
                    "date": date_txt,
                    "category": "DII",
                    "buy_value_cr": _coerce_float(row[dii_buy_col]),
                    "sell_value_cr": _coerce_float(row[dii_sell_col]),
                    "net_value_cr": _coerce_float(row[dii_net_col]),
                }
            )
        return rows

    # Shape B: category rows
    category_col = pick_col("category", "investor_category", "client_type", "clienttype")
    buy_col = pick_col("buy_value_cr", "buyvaluecr", "buy_value", "buyvalue", "buy")
    sell_col = pick_col("sell_value_cr", "sellvaluecr", "sell_value", "sellvalue", "sell")
    net_col = pick_col("net_value_cr", "netvaluecr", "net_value", "netvalue", "net")

    if not all([date_col, category_col, buy_col, sell_col, net_col]):
        print(f"⚠️ Unrecognized nse_fiidii columns: {list(df.columns)}")
        return []

    frame = df.copy()
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce").dt.date
    frame = frame.dropna(subset=[date_col])
    frame[category_col] = frame[category_col].astype(str)
    frame = frame[
        frame[category_col].str.upper().str.contains("FII|FPI|DII", regex=True, na=False)
    ].sort_values(by=date_col, ascending=False)

    latest_three_dates = list(dict.fromkeys(frame[date_col].tolist()))[:3]
    frame = frame[frame[date_col].isin(latest_three_dates)]

    for _, row in frame.iterrows():
        d = row[date_col]
        date_txt = d.isoformat() if hasattr(d, "isoformat") else str(d)
        category_txt = str(row[category_col]).upper()
        normalized_category = "FII/FPI" if ("FII" in category_txt or "FPI" in category_txt) else "DII"
        rows.append(
            {
                "date": date_txt,
                "category": normalized_category,
                "buy_value_cr": _coerce_float(row[buy_col]),
                "sell_value_cr": _coerce_float(row[sell_col]),
                "net_value_cr": _coerce_float(row[net_col]),
            }
        )
    return rows


def _fetch_latest_fii_dii_cash_from_db(conn: Connection) -> List[Dict[str, Any]]:
    query = text(
        """
        WITH recent_dates AS (
            SELECT DISTINCT date
            FROM fii_dii_cash_data
            ORDER BY date DESC
            LIMIT 3
        )
        SELECT
            c.date,
            c.category,
            c.buy_value_cr,
            c.sell_value_cr,
            c.net_value_cr
        FROM fii_dii_cash_data c
        INNER JOIN recent_dates rd ON rd.date = c.date
        ORDER BY c.date DESC, c.category
        """
    )
    rows = conn.execute(query).mappings().all()
    return [dict(row) for row in rows]


def fetch_latest_fii_dii_cash(conn: Connection) -> List[Dict[str, Any]]:
    nse_rows = _extract_cash_rows_from_nsepython()
    if nse_rows:
        return nse_rows
    return _fetch_latest_fii_dii_cash_from_db(conn)


def build_fii_dii_cash_snapshot(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        raw_date = row.get("date")
        if is_null(raw_date):
            date_value = None
        elif hasattr(raw_date, "isoformat"):
            date_value = raw_date.isoformat()
        else:
            date_value = str(raw_date)
        normalized_rows.append(
            {
                "date": date_value,
                "category": str(row["category"]) if not is_null(row.get("category")) else None,
                "buy_value_cr": float(row["buy_value_cr"]) if not is_null(row.get("buy_value_cr")) else None,
                "sell_value_cr": float(row["sell_value_cr"]) if not is_null(row.get("sell_value_cr")) else None,
                "net_value_cr": float(row["net_value_cr"]) if not is_null(row.get("net_value_cr")) else None,
            }
        )
    available_dates = sorted({str(r["date"]) for r in normalized_rows if r.get("date")}, reverse=True)
    latest_date = available_dates[0] if available_dates else None
    previous_date = available_dates[1] if len(available_dates) > 1 else None
    third_date = available_dates[2] if len(available_dates) > 2 else None

    latest_records = [r for r in normalized_rows if r.get("date") == latest_date]
    previous_records = [r for r in normalized_rows if r.get("date") == previous_date]
    lookback_records = [
        r for r in normalized_rows if r.get("date") in {latest_date, previous_date, third_date}
    ]

    return {
        "records": latest_records,
        "previous_records": previous_records,
        "lookback_records": lookback_records,
        "latest_date": latest_date,
        "previous_date": previous_date,
        "third_date": third_date,
        "generated_at": utc_now().isoformat(),
    }


def load_treemap_symbols(index_key: str) -> List[str]:
    filename = TREEMAP_INDEX_FILE_MAP.get(index_key)
    if not filename:
        raise RuntimeError(f"Unsupported treemap index key: {index_key}")

    return load_symbols_from_file(INDEX_LISTS_DIR / filename)


def validate_treemap_index_configuration() -> None:
    map_keys = set(TREEMAP_INDEX_FILE_MAP.keys())
    expected_keys = set(TREEMAP_EXPECTED_INDEX_KEYS)
    if map_keys != expected_keys:
        missing_in_map = sorted(expected_keys - map_keys)
        extra_in_map = sorted(map_keys - expected_keys)
        raise RuntimeError(
            "Treemap index map mismatch. "
            f"missing_in_map={missing_in_map}, extra_in_map={extra_in_map}"
        )

    missing_files = [
        filename
        for filename in TREEMAP_INDEX_FILE_MAP.values()
        if not (INDEX_LISTS_DIR / filename).exists()
    ]
    if missing_files:
        raise RuntimeError(
            f"Missing treemap index files in {INDEX_LISTS_DIR}: {sorted(missing_files)}"
        )


def fetch_treemap_companies(conn: Connection, symbols: List[str]) -> List[Dict[str, Any]]:
    query = text(
        """
        WITH requested AS (
            SELECT UNNEST(CAST(:symbols AS TEXT[])) AS ticker
        ),
        latest_two AS (
            SELECT
                o.tradingsymbol,
                o.close,
                ROW_NUMBER() OVER (PARTITION BY o.tradingsymbol ORDER BY o.ts DESC) AS rn
            FROM ohlcv o
            WHERE o.interval = '1day'
              AND o.tradingsymbol = ANY(:symbols)
        ),
        latest AS (
            SELECT
                tradingsymbol,
                MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
                MAX(CASE WHEN rn = 2 THEN close END) AS prev_close
            FROM latest_two
            GROUP BY tradingsymbol
        )
        SELECT
            r.ticker,
            COALESCE(NULLIF(i.name, ''), r.ticker) AS name,
            i.sector,
            CASE
                WHEN i.total_shares_held IS NOT NULL AND l.latest_close IS NOT NULL
                THEN i.total_shares_held * l.latest_close
                ELSE NULL
            END AS market_cap,
            l.latest_close AS last_close,
            CASE
                WHEN l.prev_close IS NOT NULL AND l.prev_close > 0
                THEN ((l.latest_close - l.prev_close) / l.prev_close) * 100
                ELSE NULL
            END AS pct_change
        FROM requested r
        LEFT JOIN instruments i ON i.tradingsymbol = r.ticker
        LEFT JOIN latest l ON l.tradingsymbol = r.ticker
        """
    )
    rows = conn.execute(query, {"symbols": symbols}).mappings().all()
    return [dict(row) for row in rows]


def build_treemap_snapshot(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_market_cap = sum(safe_float(row.get("market_cap")) or 0 for row in rows)
    payload = []
    for row in rows:
        market_cap = safe_float(row.get("market_cap"))
        payload.append(
            {
                "ticker": row.get("ticker"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "market_cap": market_cap,
                "weight": (market_cap / total_market_cap) if market_cap and total_market_cap > 0 else None,
                "last_close": safe_float(row.get("last_close")),
                "pct_change_1d": safe_float(row.get("pct_change")),
            }
        )
    payload.sort(key=lambda x: (x["market_cap"] or 0), reverse=True)
    return {"nodes": payload, "generated_at": utc_now().isoformat()}


def fetch_treemap_companies_list(conn: Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            WITH latest_close AS (
                SELECT
                    o.tradingsymbol,
                    o.close,
                    ROW_NUMBER() OVER (PARTITION BY o.tradingsymbol ORDER BY o.ts DESC) AS rn
                FROM ohlcv o
                WHERE o.interval = '1day'
            ),
            latest AS (
                SELECT tradingsymbol, close AS latest_close
                FROM latest_close
                WHERE rn = 1
            ),
            ranked AS (
                SELECT
                    i.tradingsymbol,
                    COALESCE(NULLIF(i.name, ''), i.tradingsymbol) AS company_name,
                    i.sector,
                    CASE
                        WHEN i.total_shares_held IS NOT NULL AND l.latest_close IS NOT NULL
                        THEN i.total_shares_held * l.latest_close
                        ELSE NULL
                    END AS market_cap,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            CASE
                                WHEN i.total_shares_held IS NOT NULL AND l.latest_close IS NOT NULL
                                THEN i.total_shares_held * l.latest_close
                                ELSE NULL
                            END DESC NULLS LAST
                    ) AS mc_rank
                FROM instruments i
                LEFT JOIN latest l ON l.tradingsymbol = i.tradingsymbol
            )
            SELECT tradingsymbol, company_name, sector, market_cap, (mc_rank <= 50) AS is_nifty
            FROM ranked
            """
        )
    ).mappings().all()
    return [dict(row) for row in rows]


def fetch_treemap_data_status(conn: Connection) -> Dict[str, Any]:
    companies_count = conn.execute(
        text(
            """
            WITH latest_close AS (
                SELECT
                    o.tradingsymbol,
                    o.close,
                    ROW_NUMBER() OVER (PARTITION BY o.tradingsymbol ORDER BY o.ts DESC) AS rn
                FROM ohlcv o
                WHERE o.interval = '1day'
            ),
            latest AS (
                SELECT tradingsymbol, close AS latest_close
                FROM latest_close
                WHERE rn = 1
            )
            SELECT COUNT(*)
            FROM instruments i
            LEFT JOIN latest l ON l.tradingsymbol = i.tradingsymbol
            WHERE i.total_shares_held IS NOT NULL
              AND l.latest_close IS NOT NULL
              AND (i.total_shares_held * l.latest_close) > 0
            """
        )
    ).scalar() or 0
    latest_date = conn.execute(text("SELECT MAX(DATE(ts)) FROM ohlcv WHERE interval = '1day'")).scalar()
    return {
        "companies_count": int(companies_count),
        "latest_ohlc_date": latest_date.isoformat() if latest_date else None,
        "status": "ready" if companies_count > 0 else "no_data",
    }


def fetch_index_daily_changes(
    conn: Connection,
    symbols: List[str],
    min_date: date,
) -> Dict[str, Dict[date, float]]:
    query = text(
        """
        WITH daily AS (
            SELECT
                tradingsymbol,
                DATE(ts) AS trade_date,
                close,
                ROW_NUMBER() OVER (PARTITION BY tradingsymbol, DATE(ts) ORDER BY ts DESC) AS rn
            FROM ohlcv
            WHERE interval = '1day'
              AND tradingsymbol = ANY(:symbols)
              AND ts >= (CAST(:min_date AS date) - INTERVAL '30 days')
        ),
        latest_per_day AS (
            SELECT tradingsymbol, trade_date, close
            FROM daily
            WHERE rn = 1
        ),
        changes AS (
            SELECT
                tradingsymbol,
                trade_date,
                close,
                LAG(close) OVER (PARTITION BY tradingsymbol ORDER BY trade_date) AS prev_close
            FROM latest_per_day
        )
        SELECT
            tradingsymbol,
            trade_date,
            CASE
                WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                ELSE ROUND(((close - prev_close) / prev_close) * 100.0, 4)
            END AS pct_change
        FROM changes
        WHERE trade_date >= CAST(:min_date AS date)
        ORDER BY tradingsymbol, trade_date
        """
    )
    rows = conn.execute(query, {"symbols": symbols, "min_date": min_date}).mappings().all()
    payload: Dict[str, Dict[date, float]] = defaultdict(dict)
    for row in rows:
        pct = safe_float(row.get("pct_change"))
        trade_date = row.get("trade_date")
        symbol = str(row.get("tradingsymbol") or "")
        if pct is None or not symbol or trade_date is None:
            continue
        payload[symbol][trade_date] = pct
    return payload


def build_expiry_calendar_snapshot(
    conn: Connection,
    years_back: int = 3,
    weeks_per_year: int = 52,
) -> Dict[str, Any]:
    def derive_range_pct(pct: Optional[float], multiplier: float = 1.2) -> Optional[float]:
        if pct is None:
            return None
        return round(abs(pct) * multiplier, 4)

    def avg_optional(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is not None and b is not None:
            return round((a + b) / 2.0, 4)
        if a is not None:
            return a
        if b is not None:
            return b
        return None

    def last_weekday_of_month(year: int, month: int, weekday: int) -> date:
        if month == 12:
            month_end = date(year, 12, 31)
        else:
            month_end = date(year, month + 1, 1) - timedelta(days=1)
        offset = (month_end.weekday() - weekday) % 7
        return month_end - timedelta(days=offset)

    today = utc_now().date()
    holiday_fallback_lookback_days = 7
    target_years = [today.year - (years_back - 1) + i for i in range(years_back)]
    first_year = min(target_years)
    min_date = date(first_year, 1, 1) - timedelta(days=7)

    nifty_symbol = "NIFTY 50"
    sensex_symbol = "SENSEX"
    banknifty_symbol = "NIFTY BANK"
    bankex_symbol = "BANKEX"
    changes_map = fetch_index_daily_changes(
        conn,
        [nifty_symbol, sensex_symbol, banknifty_symbol, bankex_symbol],
        min_date=min_date,
    )

    def resolve_expiry_with_holiday_fallback(symbol: str, target_date: date) -> tuple[date, Optional[float]]:
        symbol_changes = changes_map.get(symbol, {})
        zero_epsilon = 1e-12

        def is_valid_pct(value: Optional[float]) -> bool:
            return value is not None and abs(value) > zero_epsilon

        # For future scheduled expiries, keep the scheduled date with empty value.
        if target_date > today:
            return target_date, None

        target_pct = symbol_changes.get(target_date)
        if is_valid_pct(target_pct):
            return target_date, target_pct

        # For holiday/non-trading expiries, or same-day 0% values, shift to the nearest
        # previous trading day with a non-zero return.
        for offset in range(1, holiday_fallback_lookback_days + 1):
            fallback_date = target_date - timedelta(days=offset)
            fallback_pct = symbol_changes.get(fallback_date)
            if is_valid_pct(fallback_pct):
                return fallback_date, fallback_pct

        return target_date, None

    monthly_cache: Dict[tuple[int, int], Dict[str, Any]] = {}
    for year in target_years:
        for month in range(1, 13):
            bn_scheduled_expiry = last_weekday_of_month(year, month, 1)  # Tuesday
            mid_scheduled_expiry = last_weekday_of_month(year, month, 2)  # Wednesday
            bx_scheduled_expiry = last_weekday_of_month(year, month, 3)  # Thursday
            bn_expiry, bn_pct = resolve_expiry_with_holiday_fallback(banknifty_symbol, bn_scheduled_expiry)
            bx_expiry, bx_pct = resolve_expiry_with_holiday_fallback(bankex_symbol, bx_scheduled_expiry)
            bn_range_pct = derive_range_pct(bn_pct, 1.2)
            bx_range_pct = derive_range_pct(bx_pct, 1.2)
            monthly_mid_pct = avg_optional(bn_pct, bx_pct)
            monthly_mid_range_pct = avg_optional(bn_range_pct, bx_range_pct)
            monthly_cache[(year, month)] = {
                "banknifty_monthly_expiry": bn_expiry.isoformat(),
                "bankex_monthly_expiry": bx_expiry.isoformat(),
                "monthly_mid_date": mid_scheduled_expiry.isoformat(),
                "banknifty_pct": bn_pct,
                "banknifty_range_pct": bn_range_pct,
                "bankex_pct": bx_pct,
                "bankex_range_pct": bx_range_pct,
                "monthly_mid_pct": monthly_mid_pct,
                "monthly_mid_range_pct": monthly_mid_range_pct,
            }

    points: List[Dict[str, Any]] = []
    as_of_dates: List[date] = []

    for year in target_years:
        first_monday = date(year, 1, 1)
        while first_monday.weekday() != 0:
            first_monday += timedelta(days=1)

        for week_index in range(weeks_per_year):
            week_start = first_monday + timedelta(weeks=week_index)
            scheduled_tuesday = week_start + timedelta(days=1)
            scheduled_wednesday = week_start + timedelta(days=2)
            scheduled_thursday = week_start + timedelta(days=3)
            tuesday, nifty_pct = resolve_expiry_with_holiday_fallback(nifty_symbol, scheduled_tuesday)
            thursday, sensex_pct = resolve_expiry_with_holiday_fallback(sensex_symbol, scheduled_thursday)
            nifty_range_pct = derive_range_pct(nifty_pct, 1.2)
            sensex_range_pct = derive_range_pct(sensex_pct, 1.2)
            mid_pct = avg_optional(nifty_pct, sensex_pct)
            mid_range_pct = avg_optional(nifty_range_pct, sensex_range_pct)

            monthly_point = monthly_cache[(week_start.year, week_start.month)]
            if nifty_pct is not None:
                as_of_dates.append(tuesday)
            if sensex_pct is not None:
                as_of_dates.append(thursday)
            if monthly_point["banknifty_pct"] is not None:
                as_of_dates.append(date.fromisoformat(monthly_point["banknifty_monthly_expiry"]))
            if monthly_point["bankex_pct"] is not None:
                as_of_dates.append(date.fromisoformat(monthly_point["bankex_monthly_expiry"]))

            points.append(
                {
                    "year": year,
                    "week_index": week_index,
                    "week_start": week_start.isoformat(),
                    "tuesday": tuesday.isoformat(),
                    "wednesday": scheduled_wednesday.isoformat(),
                    "thursday": thursday.isoformat(),
                    "nifty_pct": nifty_pct,
                    "nifty_range_pct": nifty_range_pct,
                    "sensex_pct": sensex_pct,
                    "sensex_range_pct": sensex_range_pct,
                    "mid_pct": mid_pct,
                    "mid_range_pct": mid_range_pct,
                    "banknifty_monthly_expiry": monthly_point["banknifty_monthly_expiry"],
                    "bankex_monthly_expiry": monthly_point["bankex_monthly_expiry"],
                    "monthly_mid_date": monthly_point["monthly_mid_date"],
                    "banknifty_pct": monthly_point["banknifty_pct"],
                    "banknifty_range_pct": monthly_point["banknifty_range_pct"],
                    "bankex_pct": monthly_point["bankex_pct"],
                    "bankex_range_pct": monthly_point["bankex_range_pct"],
                    "monthly_mid_pct": monthly_point["monthly_mid_pct"],
                    "monthly_mid_range_pct": monthly_point["monthly_mid_range_pct"],
                }
            )

    as_of_date = max(as_of_dates).isoformat() if as_of_dates else None
    return {
        "generated_at": utc_now().isoformat(),
        "as_of_date": as_of_date,
        "years_back": years_back,
        "weeks_per_year": weeks_per_year,
        "points": points,
    }


def run_snapshot_job(engine: Engine) -> None:
    started_at = utc_now()
    print("Starting snapshot job...")
    with engine.begin() as conn:
        # Set a longer timeout for this entire transaction
        conn.execute(text("SET LOCAL statement_timeout = '600000'"))  # 10 minutes
        
        if not try_acquire_snapshot_job_lock(conn):
            print("⚠️ Another snapshot job is already running. Exiting this run.")
            return
        had_error = False
        try:
            print("Creating snapshot table...")
            create_snapshot_table(conn)
            print("Snapshot table ready.")
            # stockstable.py (DB portion only; Redis live updates stay runtime-only)
            print("Fetching stockstable raw data...")
            raw_data = fetch_stockstable_raw(conn)
            print(f"Building stockstable snapshot from {len(raw_data)} rows...")
            stockstable = build_stockstable_snapshot(raw_data)
            print("Upserting stockstable:dataset...")
            upsert_snapshot_preserve_last_non_empty(conn, "stockstable:dataset", stockstable)
            print("Upserting stockstable:sectors...")
            upsert_snapshot_preserve_last_non_empty(
                conn,
                "stockstable:sectors",
                {"sectors": stockstable["sectors"], "generated_at": stockstable["generated_at"]},
            )
            print("Stockstable snapshots complete.")

            # etfstable.py (same shape as stockstable, filtered by ETF symbol list)
            if ETF_LIST_FILE.exists():
                print(f"Loading ETF symbols from {ETF_LIST_FILE}...")
                etf_symbols = set(load_etf_symbols(ETF_LIST_FILE))
                print(f"Loaded {len(etf_symbols)} ETF symbols from CSV.")
                etf_raw_data = [
                    row for row in raw_data if str(row.get("symbol") or "").upper() in etf_symbols
                ]
                print(f"Building ETF snapshot from {len(etf_raw_data)} rows...")
                etfstable = build_stockstable_snapshot(etf_raw_data)
                print("Upserting etfstable:dataset...")
                upsert_snapshot_preserve_last_non_empty(conn, "etfstable:dataset", etfstable)
                print("Upserting etfstable:sectors...")
                upsert_snapshot_preserve_last_non_empty(
                    conn,
                    "etfstable:sectors",
                    {"sectors": etfstable["sectors"], "generated_at": etfstable["generated_at"]},
                )
                print("ETF snapshots complete.")
            else:
                print(f"⚠️ ETF list file not found at {ETF_LIST_FILE}. Skipping ETF snapshots.")

            # marketdashboard.py snapshots (instruments + per-symbol current day data)
            print("Building marketdashboard snapshots...")
            market_symbols = sorted(set(MARKET_DASHBOARD_ALLOWED_INDICES))
            market_instruments = fetch_marketdashboard_instruments(conn, market_symbols)
            upsert_snapshot_preserve_last_non_empty(
                conn,
                "marketdashboard:instruments",
                {"generated_at": utc_now().isoformat(), "instruments": market_instruments},
            )
            market_ohlcv_rows = fetch_marketdashboard_ohlcv_rows(conn, market_symbols)
            market_prev_close = fetch_marketdashboard_previous_close(conn, market_symbols)
            symbol_snapshots = build_marketdashboard_symbol_snapshots(
                market_symbols,
                market_ohlcv_rows,
                market_prev_close,
            )
            for symbol, payload in symbol_snapshots.items():
                upsert_snapshot_preserve_last_non_empty(
                    conn,
                    f"marketdashboard:symbol:{symbol}",
                    payload,
                    as_of_date=date.fromisoformat(payload["as_of_date"]) if payload.get("as_of_date") else None,
                )
            print(f"Marketdashboard snapshots complete ({len(symbol_snapshots)} symbols).")

            # sectortechnicals.py
            print("Fetching sector technical symbol snapshot...")
            sect_as_of, sect_rows = fetch_sectortech_symbol_snapshot(conn)
            print(f"Got {len(sect_rows)} sector technical rows for {sect_as_of}")
            upsert_snapshot(
                conn,
                "sectortechnicals:symbol_snapshot",
                {"as_of_date": sect_as_of.isoformat(), "rows": sect_rows, "generated_at": utc_now().isoformat()},
                as_of_date=sect_as_of,
            )
            upsert_snapshot(
                conn,
                "sectortechnicals:advances_declines",
                build_sectortech_advance_decline(sect_as_of, sect_rows),
                as_of_date=sect_as_of,
            )
            upsert_snapshot(
                conn,
                "sectortechnicals:breadth",
                build_sectortech_breadth(sect_as_of, sect_rows),
                as_of_date=sect_as_of,
            )
            nifty500_symbols = load_symbols_from_file(NIFTY500_FILE)
            upsert_snapshot(
                conn,
                "sectortechnicals:top_movers",
                build_sectortech_top_movers(fetch_sectortech_top_movers(conn, nifty500_symbols), limit=100),
                as_of_date=sect_as_of,
            )

            # sector_advance_decline.py (precompute all supported intervals/lookbacks)
            intervals = ["day", "5minute", "15minute", "30minute", "60minute"]
            lookback_period_options = [1, 2, 3, 4, 5]
            sector_indices_rows = fetch_sector_indices(conn)
            symbols = sorted({str(row["symbol"]) for row in sector_indices_rows if row.get("symbol")})
            base_metrics = fetch_symbol_latest_previous(conn, symbols, interval="day", lookback_periods=1)
            base_sector_adv = build_sector_advance_decline_snapshots(
                sector_index_rows=sector_indices_rows,
                symbol_metrics=base_metrics,
                interval="day",
                lookback_periods=1,
            )
            upsert_snapshot(conn, "sector_advance_decline:sector_list", base_sector_adv["sector_list"])
            upsert_snapshot(conn, "sector_advance_decline:sector_stocks", base_sector_adv["sector_stocks"])
            for interval in intervals:
                for lookback_periods in lookback_period_options:
                    symbol_metrics = fetch_symbol_latest_previous(
                        conn,
                        symbols,
                        interval=interval,
                        lookback_periods=lookback_periods,
                    )
                    sector_adv = build_sector_advance_decline_snapshots(
                        sector_index_rows=sector_indices_rows,
                        symbol_metrics=symbol_metrics,
                        interval=interval,
                        lookback_periods=lookback_periods,
                    )
                    upsert_snapshot(
                        conn,
                        f"sector_advance_decline:by_index:{interval}:lookback_{lookback_periods}",
                        sector_adv["advance_decline_by_index"],
                    )
                    upsert_snapshot(
                        conn,
                        f"sector_advance_decline:by_sector:{interval}:lookback_{lookback_periods}",
                        sector_adv["advance_decline_by_sector"],
                    )
                    upsert_snapshot(
                        conn,
                        f"sector_advance_decline:all:{interval}:lookback_{lookback_periods}",
                        sector_adv["all_sectors_advance_decline"],
                    )

                    if lookback_periods == 1:
                        upsert_snapshot(
                            conn,
                            f"sector_advance_decline:all:{interval}",
                            sector_adv["all_sectors_advance_decline"],
                        )

            # fii_dii.py (DB-backed pieces)
            fii_df = fetch_fii_dii_flow(conn)
            oi_df = fetch_participant_oi(conn)
            upsert_snapshot(
                conn,
                "fii_dii:flow",
                build_fii_dii_flow_snapshot(fii_df, oi_df),
            )
            cash_snapshot = build_fii_dii_cash_snapshot(fetch_latest_fii_dii_cash(conn))
            cash_as_of = date.fromisoformat(cash_snapshot["latest_date"]) if cash_snapshot.get("latest_date") else None
            upsert_snapshot(
                conn,
                "fii_dii:cash_latest",
                cash_snapshot,
                as_of_date=cash_as_of,
            )

            # treemap.py
            validate_treemap_index_configuration()
            for index_key in TREEMAP_INDEX_FILE_MAP:
                treemap_payload = build_treemap_snapshot(fetch_treemap_companies(conn, load_treemap_symbols(index_key)))
                upsert_snapshot_preserve_last_non_empty(conn, f"treemap:{index_key}", treemap_payload)
            upsert_snapshot_preserve_last_non_empty(
                conn,
                "treemap:companies",
                {"items": fetch_treemap_companies_list(conn), "generated_at": utc_now().isoformat()},
            )
            upsert_snapshot(
                conn,
                "treemap:data_status",
                {**fetch_treemap_data_status(conn), "generated_at": utc_now().isoformat()},
            )

            # expiry_calendar.py
            upsert_snapshot(
                conn,
                "landing_page:expiry_calendar",
                build_expiry_calendar_snapshot(conn, years_back=3, weeks_per_year=52),
            )

            upsert_snapshot(
                conn,
                "landing_page:job_meta",
                {
                    "started_at": started_at.isoformat(),
                    "completed_at": utc_now().isoformat(),
                    "status": "success",
                },
            )
        except BaseException:
            had_error = True
            raise
        finally:
            try:
                release_snapshot_job_lock(conn)
            except Exception as exc:
                if had_error:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        release_snapshot_job_lock(conn)
                    except Exception as retry_exc:
                        print(f"⚠️ Failed to release advisory lock cleanly: {retry_exc}")
                else:
                    print(f"⚠️ Failed to release advisory lock cleanly: {exc}")


def run_fii_dii_only_snapshot_job(engine: Engine) -> None:
    started_at = utc_now()
    print("Starting FII/DII-only snapshot job...")
    with engine.begin() as conn:
        if not try_acquire_snapshot_job_lock(conn):
            print("⚠️ Another snapshot job is already running. Exiting this run.")
            return
        had_error = False
        try:
            print("Creating snapshot table...")
            create_snapshot_table(conn)
            print("Snapshot table ready.")

            fii_df = fetch_fii_dii_flow(conn)
            oi_df = fetch_participant_oi(conn)
            upsert_snapshot(
                conn,
                "fii_dii:flow",
                build_fii_dii_flow_snapshot(fii_df, oi_df),
            )

            cash_snapshot = build_fii_dii_cash_snapshot(fetch_latest_fii_dii_cash(conn))
            cash_as_of = date.fromisoformat(cash_snapshot["latest_date"]) if cash_snapshot.get("latest_date") else None
            upsert_snapshot(
                conn,
                "fii_dii:cash_latest",
                cash_snapshot,
                as_of_date=cash_as_of,
            )

            upsert_snapshot(
                conn,
                "landing_page:job_meta",
                {
                    "started_at": started_at.isoformat(),
                    "completed_at": utc_now().isoformat(),
                    "status": "success",
                    "scope": "fii_dii_only",
                },
            )
            print("FII/DII-only snapshots refreshed successfully.")
        except BaseException:
            had_error = True
            raise
        finally:
            try:
                release_snapshot_job_lock(conn)
            except Exception as exc:
                if had_error:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        release_snapshot_job_lock(conn)
                    except Exception as retry_exc:
                        print(f"⚠️ Failed to release advisory lock cleanly: {retry_exc}")
                else:
                    print(f"⚠️ Failed to release advisory lock cleanly: {exc}")


def run_expiry_calendar_only_snapshot_job(engine: Engine) -> None:
    started_at = utc_now()
    print("Starting expiry calendar-only snapshot job...")
    with engine.begin() as conn:
        if not try_acquire_snapshot_job_lock(conn):
            print("⚠️ Another snapshot job is already running. Exiting this run.")
            return
        had_error = False
        try:
            print("Creating snapshot table...")
            create_snapshot_table(conn)
            print("Snapshot table ready.")

            # expiry_calendar.py
            print("Building expiry calendar snapshot...")
            upsert_snapshot(
                conn,
                "landing_page:expiry_calendar",
                build_expiry_calendar_snapshot(conn, years_back=3, weeks_per_year=52),
            )
            print("Expiry calendar snapshot refreshed successfully.")

            upsert_snapshot(
                conn,
                "landing_page:job_meta",
                {
                    "started_at": started_at.isoformat(),
                    "completed_at": utc_now().isoformat(),
                    "status": "success",
                    "scope": "expiry_calendar_only",
                },
            )
            print("Expiry calendar-only snapshots refreshed successfully.")
        except BaseException:
            had_error = True
            raise
        finally:
            try:
                release_snapshot_job_lock(conn)
            except Exception as exc:
                if had_error:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        release_snapshot_job_lock(conn)
                    except Exception as retry_exc:
                        print(f"⚠️ Failed to release advisory lock cleanly: {retry_exc}")
                else:
                    print(f"⚠️ Failed to release advisory lock cleanly: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate landing page snapshots")
    parser.add_argument(
        "--only",
        choices=["all", "fii_dii", "expiry_calendar"],
        default="all",
        help="Run only a subset of snapshot jobs",
    )
    args = parser.parse_args()

    print("Run at UTC:", utc_now())
    print(f"Query timeout: {QUERY_TIMEOUT_SECONDS} seconds")
    print(f"SCRIPT_DIR: {SCRIPT_DIR}")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"INDEX_LISTS_DIR: {INDEX_LISTS_DIR}")
    print(f"NIFTY500_FILE: {NIFTY500_FILE}")
    print(f"NIFTY500_FILE exists: {NIFTY500_FILE.exists()}")
    print(f"ETF_LIST_FILE: {ETF_LIST_FILE}")
    print(f"ETF_LIST_FILE exists: {ETF_LIST_FILE.exists()}")
    engine = create_engine(
        DATABASE_URL, 
        pool_pre_ping=True,
        connect_args={"options": f"-c statement_timeout={QUERY_TIMEOUT_SECONDS * 1000}"}
    )
    try:
        if args.only == "fii_dii":
            run_fii_dii_only_snapshot_job(engine)
        elif args.only == "expiry_calendar":
            run_expiry_calendar_only_snapshot_job(engine)
        else:
            run_snapshot_job(engine)
            print("Landing page snapshots refreshed successfully.")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
