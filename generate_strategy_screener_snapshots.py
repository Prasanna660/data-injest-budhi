#!/usr/bin/env python3
"""
Generate strategy screener snapshots for fast API reads.

Writes JSON payloads into dedicated table `strategy_screener_snapshots`.

Current implemented screeners:
- super_breakout
- custom_dma
- psar

Future strategy screeners can be added via SCREENER_BUILDERS.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import warnings
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError, PendingRollbackError
from ta.trend import PSARIndicator

DEFAULT_DATABASE_URL = (
    "postgresql://postgres:zKvzg4nrnigizkpbc1fN@"
    "budhi.cjwye68sytlr.ap-south-1.rds.amazonaws.com:5432/postgres"
)
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

SNAPSHOT_JOB_LOCK_KEY = int(os.getenv("STRATEGY_SCREENER_SNAPSHOT_JOB_LOCK_KEY", "90210427"))
UPSERT_RETRY_ATTEMPTS = int(os.getenv("STRATEGY_SCREENER_SNAPSHOT_UPSERT_RETRY_ATTEMPTS", "5"))
UPSERT_RETRY_BASE_SECONDS = float(os.getenv("STRATEGY_SCREENER_SNAPSHOT_UPSERT_RETRY_BASE_SECONDS", "0.25"))

SCRIPT_DIR = Path(__file__).resolve().parent  # /home/ec2-user
BASE_DIR = SCRIPT_DIR    
INDEX_FILE = BASE_DIR / "data" / "index_lists" / "nifty500.txt"
INDEX_DIR = BASE_DIR / "data" / "index_lists"

ATR_PERIOD = 14
SUMMARY_LOOKBACK_DAYS = 260
MINI_CANDLES = 30
DETAIL_LOOKBACK_DAYS = 420
SB_HOURLY_LOOKBACK_DAYS = int(os.getenv("STRATEGY_SCREENER_SB_HOURLY_LOOKBACK_DAYS", "180"))
SB_CHUNK_SIZE = int(os.getenv("STRATEGY_SCREENER_SB_CHUNK_SIZE", "40"))
DMA_SUMMARY_SET_INDEX = 0
DMA_HOURLY_LOOKBACK_DAYS = 520
DMA_CHUNK_SIZE = int(os.getenv("STRATEGY_SCREENER_DMA_CHUNK_SIZE", "40"))
PSAR_AF_START = float(os.getenv("STRATEGY_SCREENER_PSAR_AF_START", "0.02"))
PSAR_AF_INCREMENT = float(os.getenv("STRATEGY_SCREENER_PSAR_AF_INCREMENT", "0.02"))
PSAR_AF_MAX = float(os.getenv("STRATEGY_SCREENER_PSAR_AF_MAX", "0.2"))
PSAR_HOURLY_LOOKBACK_DAYS = int(os.getenv("STRATEGY_SCREENER_PSAR_HOURLY_LOOKBACK_DAYS", "180"))
PSAR_CHUNK_SIZE = int(os.getenv("STRATEGY_SCREENER_PSAR_CHUNK_SIZE", "40"))
DMA_CONFIGS = [
    {"hourly": 50, "daily": 10, "label": "8_DMA"},
    {"hourly": 100, "daily": 20, "label": "16_DMA"},
    {"hourly": 200, "daily": 50, "label": "32_DMA"},
]


def compute_supertrend(df: pd.DataFrame, atr_period: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    data = df.copy()
    data["prev_close"] = data["close"].shift(1)
    tr = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - data["prev_close"]).abs(),
            (data["low"] - data["prev_close"]).abs(),
        ],
        axis=1,
    ).max(axis=1)

    data["atr"] = tr.rolling(atr_period).mean()
    hl2 = (data["high"] + data["low"]) / 2
    data["upperband"] = hl2 + multiplier * data["atr"]
    data["lowerband"] = hl2 - multiplier * data["atr"]

    final_upper = []
    final_lower = []
    for i in range(len(data)):
        if i == 0:
            final_upper.append(data["upperband"].iloc[i])
            final_lower.append(data["lowerband"].iloc[i])
        else:
            final_upper.append(
                min(data["upperband"].iloc[i], final_upper[i - 1])
                if data["close"].iloc[i - 1] <= final_upper[i - 1]
                else data["upperband"].iloc[i]
            )
            final_lower.append(
                max(data["lowerband"].iloc[i], final_lower[i - 1])
                if data["close"].iloc[i - 1] >= final_lower[i - 1]
                else data["lowerband"].iloc[i]
            )

    data["final_upper"] = final_upper
    data["final_lower"] = final_lower

    supertrend = []
    direction = []
    for i in range(len(data)):
        if i == 0:
            supertrend.append(np.nan)
            direction.append(np.nan)
        else:
            if data["close"].iloc[i] > data["final_upper"].iloc[i - 1]:
                supertrend.append(data["final_lower"].iloc[i])
                direction.append(1)
            elif data["close"].iloc[i] < data["final_lower"].iloc[i - 1]:
                supertrend.append(data["final_upper"].iloc[i])
                direction.append(-1)
            else:
                supertrend.append(supertrend[i - 1])
                direction.append(direction[i - 1])

    data["supertrend"] = supertrend
    data["direction"] = direction
    return data.dropna(subset=["supertrend", "direction"])


def detect_breakouts(df: pd.DataFrame) -> List[Dict[str, Any]]:
    breakouts: List[Dict[str, Any]] = []
    for i in range(1, len(df)):
        prev_dir = df.iloc[i - 1]["direction"]
        curr_dir = df.iloc[i]["direction"]
        if prev_dir != curr_dir:
            breakouts.append(
                {
                    "ts": df.index[i],
                    "direction_from": int(prev_dir),
                    "direction_to": int(curr_dir),
                }
            )
    return breakouts


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def json_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def create_snapshot_table(conn: Connection) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS strategy_screener_snapshots (
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
            CREATE INDEX IF NOT EXISTS idx_strategy_screener_snapshots_as_of
                ON strategy_screener_snapshots (as_of_date);
            """
        )
    )


def _is_retryable_snapshot_error(exc: OperationalError) -> bool:
    code = getattr(getattr(exc, "orig", None), "pgcode", None)
    return code in {"40P01", "40001"}


def upsert_snapshot(conn: Connection, key: str, payload: Dict[str, Any], as_of_date: date | None = None) -> None:
    delete_statement = text(
        """
        DELETE FROM strategy_screener_snapshots
        WHERE snapshot_key = :snapshot_key;
        """
    )
    insert_statement = text(
        """
        INSERT INTO strategy_screener_snapshots (
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
            sleep_seconds = UPSERT_RETRY_BASE_SECONDS * (2**attempt)
            print(
                f"Retryable DB error while upserting {key}. "
                f"Retry {attempt + 1}/{UPSERT_RETRY_ATTEMPTS} in {sleep_seconds:.2f}s"
            )
            time.sleep(sleep_seconds)


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


def load_nifty500_symbols() -> List[str]:
    if not INDEX_FILE.exists():
        return []
    symbols: List[str] = []
    for line in INDEX_FILE.read_text(encoding="utf-8").splitlines():
        sym = line.strip().upper()
        if sym:
            symbols.append(sym)
    return symbols


def _display_name_from_index_filename(stem: str) -> str:
    normalized = stem.replace("_", " ").strip()
    if not normalized:
        return stem
    parts = [part.upper() if part.lower() == "nifty" else part.title() for part in normalized.split()]
    return " ".join(parts)


def load_index_memberships() -> Tuple[List[str], Dict[str, List[str]], List[Dict[str, Any]]]:
    if not INDEX_DIR.exists():
        symbols = load_nifty500_symbols()
        return symbols, {}, []

    index_files = sorted(p for p in INDEX_DIR.glob("*.txt") if p.is_file())
    if not index_files:
        symbols = load_nifty500_symbols()
        return symbols, {}, []

    symbols_seen: Dict[str, None] = {}
    symbol_to_indices: Dict[str, List[str]] = {}
    index_filters: List[Dict[str, Any]] = []

    for file_path in index_files:
        index_id = file_path.stem.strip().lower()
        if not index_id:
            continue
        file_symbols: List[str] = []
        try:
            lines = file_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []
        for line in lines:
            sym = line.strip().upper()
            if not sym:
                continue
            file_symbols.append(sym)
            symbols_seen.setdefault(sym, None)
            symbol_to_indices.setdefault(sym, []).append(index_id)
        index_filters.append(
            {
                "id": index_id,
                "name": _display_name_from_index_filename(index_id),
                "count": len(set(file_symbols)),
            }
        )

    for symbol, ids in symbol_to_indices.items():
        symbol_to_indices[symbol] = sorted(set(ids))

    symbols = list(symbols_seen.keys())
    if not symbols:
        symbols = load_nifty500_symbols()

    return symbols, symbol_to_indices, index_filters


def serialize_ts(ts_value: Any) -> str:
    return pd.to_datetime(ts_value).isoformat()


def calc_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def read_daily_ohlc_for_symbols(db_bind, symbols: List[str], lookback_days: int) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "ts", "open", "high", "low", "close"])

    query = (
        text(
            """
            SELECT tradingsymbol AS symbol, ts, open, high, low, close
            FROM ohlcv
            WHERE interval = '1day'
              AND tradingsymbol IN :symbols
              AND ts >= NOW() - (:lookback_days || ' days')::interval
            ORDER BY tradingsymbol, ts
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    return pd.read_sql(query, db_bind, params={"symbols": symbols, "lookback_days": lookback_days})


def read_hourly_close_for_symbols(db_bind, symbols: List[str], lookback_days: int) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "ts", "close"])

    query = (
        text(
            """
            SELECT tradingsymbol AS symbol, ts, close
            FROM ohlcv
            WHERE interval = '60minute'
              AND tradingsymbol IN :symbols
              AND ts >= NOW() - (:lookback_days || ' days')::interval
            ORDER BY tradingsymbol, ts
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    return pd.read_sql(query, db_bind, params={"symbols": symbols, "lookback_days": lookback_days})


def read_hourly_ohlc_for_symbols(db_bind, symbols: List[str], lookback_days: int) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "ts", "open", "high", "low", "close"])

    query = (
        text(
            """
            SELECT tradingsymbol AS symbol, ts, open, high, low, close
            FROM ohlcv
            WHERE interval = '60minute'
              AND tradingsymbol IN :symbols
              AND ts >= NOW() - (:lookback_days || ' days')::interval
            ORDER BY tradingsymbol, ts
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    return pd.read_sql(query, db_bind, params={"symbols": symbols, "lookback_days": lookback_days})


def _read_sql_for_meta(db_bind, query, params: Dict[str, Any]) -> pd.DataFrame:
    if isinstance(db_bind, Connection):
        # Keep metadata reads out of the main write transaction so a metadata
        # query failure does not poison the snapshot transaction.
        with db_bind.engine.connect() as read_conn:
            return pd.read_sql(query, read_conn, params=params)
    return pd.read_sql(query, db_bind, params=params)


def _resolve_instruments_market_cap_column(db_bind) -> str | None:
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'instruments'
          AND column_name IN ('market_cap', 'total_shares_held')
        """
    )
    try:
        df = _read_sql_for_meta(db_bind, query, params={})
    except Exception:
        return None
    cols = {str(v).strip().lower() for v in df.get("column_name", pd.Series(dtype=str)).tolist()}
    if "market_cap" in cols:
        return "market_cap"
    if "total_shares_held" in cols:
        return "total_shares_held"
    return None


def load_symbol_meta(
    db_bind,
    symbols: List[str],
    index_memberships: Dict[str, List[str]] | None = None,
) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    screener_query = (
        text(
            """
            SELECT symbol, name, sector
            FROM screener_symbols
            WHERE symbol IN :symbols
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    market_cap_column = _resolve_instruments_market_cap_column(db_bind)
    market_cap_expr = f"{market_cap_column} AS market_cap" if market_cap_column else "NULL::double precision AS market_cap"
    instruments_query = (
        text(
            f"""
            SELECT tradingsymbol AS symbol, name, sector, {market_cap_expr}
            FROM instruments
            WHERE tradingsymbol IN :symbols
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )

    screener_df = pd.DataFrame(columns=["symbol", "name", "sector"])
    instruments_df = pd.DataFrame(columns=["symbol", "name", "sector", "market_cap"])
    try:
        screener_df = _read_sql_for_meta(db_bind, screener_query, params={"symbols": symbols})
    except Exception:
        pass
    try:
        instruments_df = _read_sql_for_meta(db_bind, instruments_query, params={"symbols": symbols})
    except Exception:
        pass

    instruments_map: Dict[str, Dict[str, Any]] = {}
    for _, row in instruments_df.iterrows():
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        market_cap_val = row.get("market_cap")
        instruments_map[sym] = {
            "name": str(row.get("name") or sym),
            "sector": str(row.get("sector") or "Unknown"),
            "market_cap": float(market_cap_val) if pd.notna(market_cap_val) else None,
        }

    meta: Dict[str, Dict[str, Any]] = {}
    for _, row in screener_df.iterrows():
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        instrument_meta = instruments_map.get(sym, {})
        meta[sym] = {
            "name": str(row.get("name") or instrument_meta.get("name") or sym),
            "sector": str(row.get("sector") or instrument_meta.get("sector") or "Unknown"),
            "market_cap": instrument_meta.get("market_cap"),
            "indices": list((index_memberships or {}).get(sym, [])),
        }

    for symbol in symbols:
        sym = str(symbol or "").upper()
        if not sym or sym in meta:
            continue
        instrument_meta = instruments_map.get(sym, {})
        meta[sym] = {
            "name": str(instrument_meta.get("name") or sym),
            "sector": str(instrument_meta.get("sector") or "Unknown"),
            "market_cap": instrument_meta.get("market_cap"),
            "indices": list((index_memberships or {}).get(sym, [])),
        }
    return meta


def build_signal_payload(symbol: str, frame: pd.DataFrame, meta: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    frame = frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values("ts")

    if frame.empty:
        return {
            "symbol": symbol,
            "name": meta.get(symbol, {}).get("name", symbol),
            "sector": meta.get(symbol, {}).get("sector", "Unknown"),
            "market_cap": meta.get(symbol, {}).get("market_cap"),
            "indices": list(meta.get(symbol, {}).get("indices") or []),
            "signal": "NEUTRAL",
            "signal_strength": 0.0,
            "price": None,
            "change": None,
            "change_pct": None,
            "upper_band": None,
            "lower_band": None,
            "high_14d": None,
            "atr_14": None,
            "direction": None,
            "latest_breakout": None,
            "mini_candles": [],
        }

    idx = frame.set_index("ts")
    st = compute_supertrend(idx)
    atr14 = calc_atr(idx, period=ATR_PERIOD)
    high14 = idx["high"].rolling(14).max()

    latest_close = float(idx["close"].iloc[-1])
    prev_close = float(idx["close"].iloc[-2]) if len(idx) > 1 else latest_close
    change = latest_close - prev_close
    change_pct = (change / prev_close * 100.0) if prev_close else 0.0

    latest_breakout = None
    if not st.empty:
        breakouts = detect_breakouts(st)
        if breakouts:
            last = breakouts[-1]
            latest_breakout = {
                "ts": serialize_ts(last["ts"]),
                "direction_from": int(last["direction_from"]),
                "direction_to": int(last["direction_to"]),
            }

    if st.empty:
        signal = "NEUTRAL"
        direction = None
        supertrend_value = np.nan
        upper_band = np.nan
        lower_band = np.nan
    else:
        latest_st = st.iloc[-1]
        direction = int(latest_st["direction"])
        supertrend_value = float(latest_st["supertrend"])
        upper_band = float(latest_st.get("final_upper", np.nan))
        lower_band = float(latest_st.get("final_lower", np.nan))
        if direction == 1:
            signal = "BUY"
        elif direction == -1:
            signal = "SELL"
        else:
            signal = "NEUTRAL"

    latest_atr = float(atr14.iloc[-1]) if len(atr14) and pd.notna(atr14.iloc[-1]) else np.nan
    latest_high14 = float(high14.iloc[-1]) if len(high14) and pd.notna(high14.iloc[-1]) else np.nan

    if pd.notna(latest_atr) and latest_atr > 0 and pd.notna(supertrend_value):
        strength = min(100.0, abs((latest_close - supertrend_value) / latest_atr) * 100.0)
    else:
        strength = 0.0

    mini = frame.tail(MINI_CANDLES)
    mini_candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in mini.itertuples(index=False)
    ]

    return {
        "symbol": symbol,
        "name": meta.get(symbol, {}).get("name", symbol),
        "sector": meta.get(symbol, {}).get("sector", "Unknown"),
        "market_cap": meta.get(symbol, {}).get("market_cap"),
        "indices": list(meta.get(symbol, {}).get("indices") or []),
        "signal": signal,
        "signal_strength": round(float(strength), 2),
        "price": round(float(latest_close), 2),
        "change": round(float(change), 2),
        "change_pct": round(float(change_pct), 2),
        "upper_band": round(float(upper_band), 2) if pd.notna(upper_band) else None,
        "lower_band": round(float(lower_band), 2) if pd.notna(lower_band) else None,
        "high_14d": round(float(latest_high14), 2) if pd.notna(latest_high14) else None,
        "atr_14": round(float(latest_atr), 2) if pd.notna(latest_atr) else None,
        "direction": direction,
        "latest_breakout": latest_breakout,
        "mini_candles": mini_candles,
    }


def build_stock_detail_payload(symbol: str, frame: pd.DataFrame, meta: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    normalized = symbol.strip().upper()
    if frame.empty:
        raise ValueError(f"No OHLC data for {normalized}")

    frame = frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values("ts")
    if frame.empty:
        raise ValueError(f"No OHLC data for {normalized}")

    idx = frame.set_index("ts")
    st = compute_supertrend(idx)
    atr14 = calc_atr(idx, period=ATR_PERIOD)
    high14 = idx["high"].rolling(14).max()
    latest = build_signal_payload(normalized, frame, meta)

    candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in frame.itertuples(index=False)
    ]

    st_indexed = (
        st[["supertrend", "direction", "final_upper", "final_lower"]].copy()
        if not st.empty
        else pd.DataFrame(columns=["supertrend", "direction", "final_upper", "final_lower"])
    )

    overlays = {
        "supertrend": [
            {"ts": serialize_ts(ts), "value": float(row["supertrend"]), "direction": int(row["direction"])}
            for ts, row in st_indexed.iterrows()
            if pd.notna(row.get("supertrend")) and pd.notna(row.get("direction"))
        ],
        "upper_band": [
            {"ts": serialize_ts(ts), "value": float(val)}
            for ts, val in st_indexed["final_upper"].items()
            if pd.notna(val)
        ],
        "lower_band": [
            {"ts": serialize_ts(ts), "value": float(val)}
            for ts, val in st_indexed["final_lower"].items()
            if pd.notna(val)
        ],
        "high_14d": [
            {"ts": serialize_ts(ts), "value": float(val)}
            for ts, val in high14.items()
            if pd.notna(val)
        ],
    }

    atr_series = [
        {"ts": serialize_ts(ts), "value": float(val)}
        for ts, val in atr14.items()
        if pd.notna(val)
    ]

    return {
        "screener": "Super Breakout",
        "symbol": normalized,
        "name": latest["name"],
        "sector": latest["sector"],
        "signal": latest["signal"],
        "signal_strength": latest["signal_strength"],
        "price": latest["price"],
        "change": latest["change"],
        "change_pct": latest["change_pct"],
        "stats": {
            "upper_band": latest["upper_band"],
            "lower_band": latest["lower_band"],
            "atr_14": latest["atr_14"],
            "high_14d": latest["high_14d"],
            "latest_breakout": latest["latest_breakout"],
        },
        "candles": candles,
        "overlays": overlays,
        "atr_14_series": atr_series,
    }


def build_super_breakout_timeframe_payload(frame: pd.DataFrame) -> Dict[str, Any]:
    if frame.empty:
        return {"candles": [], "supertrend": [], "breakouts": []}
    frame = frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values("ts")
    idx = frame.set_index("ts")
    st = compute_supertrend(idx)
    breakouts = detect_breakouts(st) if not st.empty else []
    candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in frame.itertuples(index=False)
    ]
    st_points = [
        {"ts": serialize_ts(ts), "supertrend": float(row["supertrend"]), "direction": int(row["direction"])}
        for ts, row in st.iterrows()
        if pd.notna(row.get("supertrend")) and pd.notna(row.get("direction"))
    ]
    breakout_points = [
        {
            "ts": serialize_ts(item["ts"]),
            "direction_from": int(item["direction_from"]),
            "direction_to": int(item["direction_to"]),
            "window": {"left": 3, "right": 3},
        }
        for item in breakouts
    ]
    return {"candles": candles, "supertrend": st_points, "breakouts": breakout_points}


def build_super_breakout_snapshot_bundle(db_bind) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    symbols, index_memberships, index_filters = load_index_memberships()
    if not symbols:
        raise ValueError("Index symbol lists missing")

    daily = read_daily_ohlc_for_symbols(db_bind, symbols, DETAIL_LOOKBACK_DAYS)
    hourly = read_hourly_ohlc_for_symbols(db_bind, symbols, SB_HOURLY_LOOKBACK_DAYS)
    meta = load_symbol_meta(db_bind, symbols, index_memberships=index_memberships)
    grouped = {str(sym).upper(): g.copy() for sym, g in daily.groupby("symbol", sort=False)}
    grouped_hourly = {str(sym).upper(): g.copy() for sym, g in hourly.groupby("symbol", sort=False)}

    stocks: List[Dict[str, Any]] = []
    details: Dict[str, Dict[str, Any]] = {}
    empty_frame = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])

    for symbol in symbols:
        frame = grouped.get(symbol, empty_frame)
        frame_summary = frame.tail(SUMMARY_LOOKBACK_DAYS) if not frame.empty else frame
        signal_payload = build_signal_payload(symbol, frame_summary, meta)
        stocks.append(signal_payload)
        if not frame.empty:
            detail = build_stock_detail_payload(symbol, frame, meta)
            hourly_frame = grouped_hourly.get(symbol, pd.DataFrame(columns=["ts", "open", "high", "low", "close"]))
            detail["timeframes"] = {
                "1day": build_super_breakout_timeframe_payload(frame),
                "1hour": build_super_breakout_timeframe_payload(hourly_frame),
            }
            details[symbol] = detail

    buy_count = sum(1 for s in stocks if s["signal"] == "BUY")
    sell_count = sum(1 for s in stocks if s["signal"] == "SELL")
    neutral_count = sum(1 for s in stocks if s["signal"] == "NEUTRAL")
    stocks.sort(
        key=lambda x: (
            x["signal"] != "BUY",
            x["signal"] == "NEUTRAL",
            -float(x.get("signal_strength") or 0.0),
            x["symbol"],
        )
    )

    summary = {
        "screener": "Super Breakout",
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "counts": {
            "buy": buy_count,
            "sell": sell_count,
            "neutral": neutral_count,
            "total": len(stocks),
        },
        "index_filters": index_filters,
        "stocks": stocks,
    }
    return summary, details


def build_super_breakout_snapshots_streaming(conn: Connection) -> Tuple[Dict[str, Any], int]:
    symbols, index_memberships, index_filters = load_index_memberships()
    if not symbols:
        raise ValueError("Index symbol lists missing")

    meta = load_symbol_meta(conn, symbols, index_memberships=index_memberships)
    stocks: List[Dict[str, Any]] = []
    stock_snapshots_written = 0
    empty_daily = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    empty_hourly = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    stock_prefix = "strategy_screener:super_breakout:stock:"

    for chunk_symbols in _iter_symbol_chunks(symbols, SB_CHUNK_SIZE):
        daily = read_daily_ohlc_for_symbols(conn, chunk_symbols, DETAIL_LOOKBACK_DAYS)
        hourly = read_hourly_ohlc_for_symbols(conn, chunk_symbols, SB_HOURLY_LOOKBACK_DAYS)
        grouped_daily = {str(sym).upper(): g for sym, g in daily.groupby("symbol", sort=False)}
        grouped_hourly = {str(sym).upper(): g for sym, g in hourly.groupby("symbol", sort=False)}

        for symbol in chunk_symbols:
            frame = grouped_daily.get(symbol, empty_daily)
            hourly_frame = grouped_hourly.get(symbol, empty_hourly)
            frame_summary = frame.tail(SUMMARY_LOOKBACK_DAYS) if not frame.empty else frame
            signal_payload = build_signal_payload(symbol, frame_summary, meta)
            stocks.append(signal_payload)

            if frame.empty:
                continue

            detail_payload = build_stock_detail_payload(symbol, frame, meta)
            detail_payload["timeframes"] = {
                "1day": build_super_breakout_timeframe_payload(frame),
                "1hour": build_super_breakout_timeframe_payload(hourly_frame),
            }
            upsert_snapshot(conn, f"{stock_prefix}{symbol}", detail_payload)
            stock_snapshots_written += 1

    buy_count = sum(1 for s in stocks if s["signal"] == "BUY")
    sell_count = sum(1 for s in stocks if s["signal"] == "SELL")
    neutral_count = sum(1 for s in stocks if s["signal"] == "NEUTRAL")
    stocks.sort(
        key=lambda x: (
            x["signal"] != "BUY",
            x["signal"] == "NEUTRAL",
            -float(x.get("signal_strength") or 0.0),
            x["symbol"],
        )
    )

    summary = {
        "screener": "Super Breakout",
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "counts": {
            "buy": buy_count,
            "sell": sell_count,
            "neutral": neutral_count,
            "total": len(stocks),
        },
        "index_filters": index_filters,
        "stocks": stocks,
    }
    return summary, stock_snapshots_written


def _to_date_index(series: pd.Series) -> pd.Series:
    idx = pd.to_datetime(series.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("Asia/Kolkata").tz_localize(None)
    idx = idx.normalize()
    out = series.copy()
    out.index = idx
    return out.groupby(level=0).last()


def _filter_market_hours(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return hourly
    idx = pd.to_datetime(hourly.index)
    data = hourly.copy()
    data.index = idx

    if getattr(idx, "tz", None) is not None:
        local = data.tz_convert("Asia/Kolkata")
        filtered = local.between_time("09:15", "15:30")
    else:
        filtered = data.between_time("09:15", "15:30")

    return filtered if not filtered.empty else data


def compute_custom_dma_sets(daily: pd.DataFrame, hourly: pd.DataFrame) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    hourly = _filter_market_hours(hourly)
    daily_close = _to_date_index(daily["close"])

    for cfg in DMA_CONFIGS:
        hourly_ma = hourly["close"].rolling(cfg["hourly"]).mean()
        custom_daily = _to_date_index(hourly_ma.resample("1D").last())
        daily_dma = daily_close.rolling(cfg["daily"]).mean()

        aligned = pd.DataFrame({"close": daily_close})
        aligned["custom_dma"] = custom_daily.reindex(aligned.index)
        aligned["regular_dma"] = daily_dma.reindex(aligned.index)
        aligned = aligned.dropna(subset=["custom_dma", "regular_dma"])
        aligned.index.name = "ts"

        records = []
        for ts, row in aligned.iterrows():
            records.append(
                {
                    "ts": serialize_ts(ts),
                    "close": float(row["close"]),
                    "custom_dma": float(row["custom_dma"]),
                    "regular_dma": float(row["regular_dma"]),
                }
            )

        results.append(
            {
                "label": cfg["label"],
                "hourly_period": cfg["hourly"],
                "daily_period": cfg["daily"],
                "data": records,
            }
        )

    return results


def _signal_from_dma(close_value: float, custom_dma: float, regular_dma: float) -> str:
    if close_value > custom_dma and close_value > regular_dma:
        return "BUY"
    if close_value < custom_dma and close_value < regular_dma:
        return "SELL"
    return "NEUTRAL"


def _strength_from_dma(close_value: float, custom_dma: float, regular_dma: float) -> float:
    if close_value == 0:
        return 0.0
    custom_pct = abs((close_value - custom_dma) / close_value) * 100.0
    regular_pct = abs((close_value - regular_dma) / close_value) * 100.0
    return float(min(100.0, (custom_pct + regular_pct) / 2.0))


def build_custom_dma_signal_payload(
    symbol: str,
    daily_frame: pd.DataFrame,
    hourly_frame: pd.DataFrame,
    meta: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if daily_frame.empty or hourly_frame.empty:
        return {
            "symbol": symbol,
            "name": meta.get(symbol, {}).get("name", symbol),
            "sector": meta.get(symbol, {}).get("sector", "Unknown"),
            "market_cap": meta.get(symbol, {}).get("market_cap"),
            "indices": list(meta.get(symbol, {}).get("indices") or []),
            "signal": "NEUTRAL",
            "signal_strength": 0.0,
            "price": None,
            "change": None,
            "change_pct": None,
            "dma_set_label": DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["label"],
            "hourly_period": DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["hourly"],
            "daily_period": DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["daily"],
            "custom_dma": None,
            "regular_dma": None,
            "mini_candles": [],
        }

    daily = daily_frame.copy()
    hourly = hourly_frame.copy()
    daily["ts"] = pd.to_datetime(daily["ts"])
    hourly["ts"] = pd.to_datetime(hourly["ts"])
    daily = daily.sort_values("ts")
    hourly = hourly.sort_values("ts")

    daily_idx = daily.set_index("ts")
    hourly_idx = hourly.set_index("ts")
    dma_sets = compute_custom_dma_sets(daily_idx, hourly_idx)
    target_set = dma_sets[DMA_SUMMARY_SET_INDEX] if len(dma_sets) > DMA_SUMMARY_SET_INDEX else None
    latest_dma_row = target_set["data"][-1] if target_set and target_set["data"] else None

    latest_close = float(daily["close"].iloc[-1])
    prev_close = float(daily["close"].iloc[-2]) if len(daily) > 1 else latest_close
    change = latest_close - prev_close
    change_pct = (change / prev_close * 100.0) if prev_close else 0.0

    if latest_dma_row:
        custom_dma = float(latest_dma_row["custom_dma"])
        regular_dma = float(latest_dma_row["regular_dma"])
        signal = _signal_from_dma(latest_close, custom_dma, regular_dma)
        strength = _strength_from_dma(latest_close, custom_dma, regular_dma)
    else:
        custom_dma = None
        regular_dma = None
        signal = "NEUTRAL"
        strength = 0.0

    mini = daily.tail(MINI_CANDLES)
    mini_candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in mini.itertuples(index=False)
    ]

    return {
        "symbol": symbol,
        "name": meta.get(symbol, {}).get("name", symbol),
        "sector": meta.get(symbol, {}).get("sector", "Unknown"),
        "market_cap": meta.get(symbol, {}).get("market_cap"),
        "indices": list(meta.get(symbol, {}).get("indices") or []),
        "signal": signal,
        "signal_strength": round(float(strength), 2),
        "price": round(float(latest_close), 2),
        "change": round(float(change), 2),
        "change_pct": round(float(change_pct), 2),
        "dma_set_label": target_set["label"] if target_set else DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["label"],
        "hourly_period": target_set["hourly_period"] if target_set else DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["hourly"],
        "daily_period": target_set["daily_period"] if target_set else DMA_CONFIGS[DMA_SUMMARY_SET_INDEX]["daily"],
        "custom_dma": round(float(custom_dma), 2) if custom_dma is not None else None,
        "regular_dma": round(float(regular_dma), 2) if regular_dma is not None else None,
        "mini_candles": mini_candles,
    }


def build_custom_dma_stock_detail_payload(
    symbol: str,
    daily_frame: pd.DataFrame,
    hourly_frame: pd.DataFrame,
    meta: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    normalized = symbol.strip().upper()
    if daily_frame.empty or hourly_frame.empty:
        raise ValueError(f"No OHLC data for {normalized}")

    daily = daily_frame.copy()
    hourly = hourly_frame.copy()
    daily["ts"] = pd.to_datetime(daily["ts"])
    hourly["ts"] = pd.to_datetime(hourly["ts"])
    daily = daily.sort_values("ts")
    hourly = hourly.sort_values("ts")
    daily_idx = daily.set_index("ts")
    hourly_idx = hourly.set_index("ts")
    dma_sets = compute_custom_dma_sets(daily_idx, hourly_idx)

    summary_payload = build_custom_dma_signal_payload(normalized, daily, hourly, meta)
    candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in daily.itertuples(index=False)
    ]

    normalized_sets: List[Dict[str, Any]] = []
    for dma_set in dma_sets:
        series = dma_set.get("data") or []
        if series:
            latest_row = series[-1]
            latest_close = float(latest_row["close"])
            latest_custom = float(latest_row["custom_dma"])
            latest_regular = float(latest_row["regular_dma"])
            set_signal = _signal_from_dma(latest_close, latest_custom, latest_regular)
            set_strength = _strength_from_dma(latest_close, latest_custom, latest_regular)
        else:
            latest_custom = None
            latest_regular = None
            set_signal = "NEUTRAL"
            set_strength = 0.0

        normalized_sets.append(
            {
                "label": dma_set["label"],
                "hourly_period": dma_set["hourly_period"],
                "daily_period": dma_set["daily_period"],
                "stats": {
                    "custom_dma": round(float(latest_custom), 2) if latest_custom is not None else None,
                    "regular_dma": round(float(latest_regular), 2) if latest_regular is not None else None,
                    "signal": set_signal,
                    "signal_strength": round(float(set_strength), 2),
                },
                "overlays": {
                    "custom_dma": [{"ts": row["ts"], "value": float(row["custom_dma"])} for row in series],
                    "regular_dma": [{"ts": row["ts"], "value": float(row["regular_dma"])} for row in series],
                },
            }
        )

    return {
        "screener": "Custom DMA",
        "symbol": normalized,
        "name": summary_payload["name"],
        "sector": summary_payload["sector"],
        "signal": summary_payload["signal"],
        "signal_strength": summary_payload["signal_strength"],
        "price": summary_payload["price"],
        "change": summary_payload["change"],
        "change_pct": summary_payload["change_pct"],
        "stats": {
            "dma_set_label": summary_payload["dma_set_label"],
            "hourly_period": summary_payload["hourly_period"],
            "daily_period": summary_payload["daily_period"],
            "custom_dma": summary_payload["custom_dma"],
            "regular_dma": summary_payload["regular_dma"],
        },
        "candles": candles,
        "dma_sets": normalized_sets,
        "overlays": normalized_sets[DMA_SUMMARY_SET_INDEX]["overlays"] if len(normalized_sets) > DMA_SUMMARY_SET_INDEX else {"custom_dma": [], "regular_dma": []},
    }


def build_custom_dma_snapshot_bundle(db_bind) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    symbols, index_memberships, index_filters = load_index_memberships()
    if not symbols:
        raise ValueError("Index symbol lists missing")

    daily = read_daily_ohlc_for_symbols(db_bind, symbols, DETAIL_LOOKBACK_DAYS)
    hourly = read_hourly_close_for_symbols(db_bind, symbols, DMA_HOURLY_LOOKBACK_DAYS)
    meta = load_symbol_meta(db_bind, symbols, index_memberships=index_memberships)
    daily_grouped = {str(sym).upper(): g.copy() for sym, g in daily.groupby("symbol", sort=False)}
    hourly_grouped = {str(sym).upper(): g.copy() for sym, g in hourly.groupby("symbol", sort=False)}

    stocks: List[Dict[str, Any]] = []
    details: Dict[str, Dict[str, Any]] = {}
    empty_daily = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    empty_hourly = pd.DataFrame(columns=["ts", "close"])

    for symbol in symbols:
        daily_frame = daily_grouped.get(symbol, empty_daily)
        hourly_frame = hourly_grouped.get(symbol, empty_hourly)
        frame_summary = daily_frame.tail(SUMMARY_LOOKBACK_DAYS) if not daily_frame.empty else daily_frame
        signal_payload = build_custom_dma_signal_payload(symbol, frame_summary, hourly_frame, meta)
        stocks.append(signal_payload)
        if not daily_frame.empty and not hourly_frame.empty:
            details[symbol] = build_custom_dma_stock_detail_payload(symbol, daily_frame, hourly_frame, meta)

    buy_count = sum(1 for s in stocks if s["signal"] == "BUY")
    sell_count = sum(1 for s in stocks if s["signal"] == "SELL")
    neutral_count = sum(1 for s in stocks if s["signal"] == "NEUTRAL")
    stocks.sort(
        key=lambda x: (
            x["signal"] != "BUY",
            x["signal"] == "NEUTRAL",
            -float(x.get("signal_strength") or 0.0),
            x["symbol"],
        )
    )

    summary = {
        "screener": "Custom DMA",
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "counts": {
            "buy": buy_count,
            "sell": sell_count,
            "neutral": neutral_count,
            "total": len(stocks),
        },
        "index_filters": index_filters,
        "stocks": stocks,
    }
    return summary, details


def _iter_symbol_chunks(symbols: List[str], chunk_size: int):
    safe_chunk = max(1, int(chunk_size))
    for i in range(0, len(symbols), safe_chunk):
        yield symbols[i : i + safe_chunk]


def build_custom_dma_snapshots_streaming(
    conn: Connection,
) -> Tuple[Dict[str, Any], int]:
    symbols, index_memberships, index_filters = load_index_memberships()
    if not symbols:
        raise ValueError("Index symbol lists missing")

    meta = load_symbol_meta(conn, symbols, index_memberships=index_memberships)
    stocks: List[Dict[str, Any]] = []
    stock_snapshots_written = 0
    empty_daily = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    empty_hourly = pd.DataFrame(columns=["ts", "close"])
    stock_prefix = "strategy_screener:custom_dma:stock:"

    for chunk_symbols in _iter_symbol_chunks(symbols, DMA_CHUNK_SIZE):
        daily = read_daily_ohlc_for_symbols(conn, chunk_symbols, DETAIL_LOOKBACK_DAYS)
        hourly = read_hourly_close_for_symbols(conn, chunk_symbols, DMA_HOURLY_LOOKBACK_DAYS)
        daily_grouped = {str(sym).upper(): g for sym, g in daily.groupby("symbol", sort=False)}
        hourly_grouped = {str(sym).upper(): g for sym, g in hourly.groupby("symbol", sort=False)}

        for symbol in chunk_symbols:
            daily_frame = daily_grouped.get(symbol, empty_daily)
            hourly_frame = hourly_grouped.get(symbol, empty_hourly)
            frame_summary = daily_frame.tail(SUMMARY_LOOKBACK_DAYS) if not daily_frame.empty else daily_frame
            signal_payload = build_custom_dma_signal_payload(symbol, frame_summary, hourly_frame, meta)
            stocks.append(signal_payload)

            if daily_frame.empty or hourly_frame.empty:
                continue

            detail_payload = build_custom_dma_stock_detail_payload(symbol, daily_frame, hourly_frame, meta)
            upsert_snapshot(conn, f"{stock_prefix}{symbol}", detail_payload)
            stock_snapshots_written += 1

    buy_count = sum(1 for s in stocks if s["signal"] == "BUY")
    sell_count = sum(1 for s in stocks if s["signal"] == "SELL")
    neutral_count = sum(1 for s in stocks if s["signal"] == "NEUTRAL")
    stocks.sort(
        key=lambda x: (
            x["signal"] != "BUY",
            x["signal"] == "NEUTRAL",
            -float(x.get("signal_strength") or 0.0),
            x["symbol"],
        )
    )

    summary = {
        "screener": "Custom DMA",
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "counts": {
            "buy": buy_count,
            "sell": sell_count,
            "neutral": neutral_count,
            "total": len(stocks),
        },
        "index_filters": index_filters,
        "stocks": stocks,
    }
    return summary, stock_snapshots_written


def compute_psar(
    frame: pd.DataFrame,
    af_start: float = PSAR_AF_START,
    af_increment: float = PSAR_AF_INCREMENT,
    af_max: float = PSAR_AF_MAX,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["psar", "psar_reversal", "signal", "psar_af"])

    df = frame.copy()
    df = df.sort_index()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Series.__setitem__ treating keys as positions is deprecated",
            category=FutureWarning,
        )
        indicator = PSARIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            step=af_start,
            max_step=af_max,
        )
        psar_values = indicator.psar()
    signals = pd.Series("NEUTRAL", index=df.index)
    signals = signals.mask(df["close"] > psar_values, "BUY")
    signals = signals.mask(df["close"] < psar_values, "SELL")
    reversals = signals.ne(signals.shift(1)).fillna(False)

    out = pd.DataFrame(index=df.index)
    out["psar"] = psar_values
    out["psar_reversal"] = reversals
    out["signal"] = signals
    out["psar_af"] = af_start
    return out


def _signal_strength_from_psar(close_value: float, psar_value: float | None) -> float:
    if psar_value is None or close_value == 0:
        return 0.0
    return float(min(100.0, abs((close_value - psar_value) / close_value) * 100.0 * 2.0))


def build_psar_signal_payload(
    symbol: str,
    daily_frame: pd.DataFrame,
    hourly_frame: pd.DataFrame,
    meta: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if daily_frame.empty:
        return {
            "symbol": symbol,
            "name": meta.get(symbol, {}).get("name", symbol),
            "sector": meta.get(symbol, {}).get("sector", "Unknown"),
            "market_cap": meta.get(symbol, {}).get("market_cap"),
            "indices": list(meta.get(symbol, {}).get("indices") or []),
            "signal": "NEUTRAL",
            "signal_strength": 0.0,
            "price": None,
            "change": None,
            "change_pct": None,
            "psar_1day": None,
            "psar_1hour": None,
            "reversal_1day": None,
            "reversal_1hour": None,
            "af": PSAR_AF_START,
            "max_af": PSAR_AF_MAX,
            "mini_candles": [],
        }

    daily = daily_frame.copy()
    daily["ts"] = pd.to_datetime(daily["ts"])
    daily = daily.sort_values("ts")
    daily_idx = daily.set_index("ts")
    psar_daily = compute_psar(daily_idx)

    hourly_psar_latest = None
    hourly_reversal_latest = None
    if not hourly_frame.empty:
        hourly = hourly_frame.copy()
        hourly["ts"] = pd.to_datetime(hourly["ts"])
        hourly = hourly.sort_values("ts")
        hourly_idx = hourly.set_index("ts")
        psar_hourly = compute_psar(hourly_idx)
        if not psar_hourly.empty:
            hourly_psar_latest = float(psar_hourly["psar"].iloc[-1]) if pd.notna(psar_hourly["psar"].iloc[-1]) else None
            hourly_reversal_latest = bool(psar_hourly["psar_reversal"].iloc[-1])

    latest_close = float(daily["close"].iloc[-1])
    prev_close = float(daily["close"].iloc[-2]) if len(daily) > 1 else latest_close
    change = latest_close - prev_close
    change_pct = (change / prev_close * 100.0) if prev_close else 0.0

    psar_1day = None
    reversal_1day = None
    signal = "NEUTRAL"
    if not psar_daily.empty and pd.notna(psar_daily["psar"].iloc[-1]):
        psar_1day = float(psar_daily["psar"].iloc[-1])
        reversal_1day = bool(psar_daily["psar_reversal"].iloc[-1])
        signal = str(psar_daily["signal"].iloc[-1])

    strength = _signal_strength_from_psar(latest_close, psar_1day)
    mini = daily.tail(MINI_CANDLES)
    mini_candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in mini.itertuples(index=False)
    ]

    return {
        "symbol": symbol,
        "name": meta.get(symbol, {}).get("name", symbol),
        "sector": meta.get(symbol, {}).get("sector", "Unknown"),
        "market_cap": meta.get(symbol, {}).get("market_cap"),
        "indices": list(meta.get(symbol, {}).get("indices") or []),
        "signal": signal,
        "signal_strength": round(float(strength), 2),
        "price": round(float(latest_close), 2),
        "change": round(float(change), 2),
        "change_pct": round(float(change_pct), 2),
        "psar_1day": round(float(psar_1day), 2) if psar_1day is not None else None,
        "psar_1hour": round(float(hourly_psar_latest), 2) if hourly_psar_latest is not None else None,
        "reversal_1day": reversal_1day,
        "reversal_1hour": hourly_reversal_latest,
        "af": PSAR_AF_START,
        "max_af": PSAR_AF_MAX,
        "mini_candles": mini_candles,
    }


def build_psar_stock_detail_payload(
    symbol: str,
    daily_frame: pd.DataFrame,
    hourly_frame: pd.DataFrame,
    meta: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    normalized = symbol.strip().upper()
    if daily_frame.empty:
        raise ValueError(f"No OHLC data for {normalized}")

    daily = daily_frame.copy()
    daily["ts"] = pd.to_datetime(daily["ts"])
    daily = daily.sort_values("ts")
    daily_idx = daily.set_index("ts")
    daily_psar = compute_psar(daily_idx)

    hourly_records: List[Dict[str, Any]] = []
    hourly_psar_records: List[Dict[str, Any]] = []
    hourly_latest = None
    if not hourly_frame.empty:
        hourly = hourly_frame.copy()
        hourly["ts"] = pd.to_datetime(hourly["ts"])
        hourly = hourly.sort_values("ts")
        hourly_idx = hourly.set_index("ts")
        hourly_psar = compute_psar(hourly_idx)
        hourly_records = [
            {
                "ts": serialize_ts(row.ts),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
            }
            for row in hourly.itertuples(index=False)
        ]
        hourly_psar_records = [
            {
                "ts": serialize_ts(ts),
                "value": float(row["psar"]),
                "signal": str(row["signal"]),
                "reversal": bool(row["psar_reversal"]),
                "af": float(row["psar_af"]) if pd.notna(row["psar_af"]) else None,
            }
            for ts, row in hourly_psar.iterrows()
            if pd.notna(row.get("psar"))
        ]
        if not hourly_psar.empty:
            hourly_latest = {
                "signal": str(hourly_psar["signal"].iloc[-1]),
                "psar": float(hourly_psar["psar"].iloc[-1]) if pd.notna(hourly_psar["psar"].iloc[-1]) else None,
                "reversal": bool(hourly_psar["psar_reversal"].iloc[-1]),
            }

    summary_payload = build_psar_signal_payload(normalized, daily, hourly_frame, meta)
    daily_candles = [
        {
            "ts": serialize_ts(row.ts),
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
        }
        for row in daily.itertuples(index=False)
    ]
    daily_psar_records = [
        {
            "ts": serialize_ts(ts),
            "value": float(row["psar"]),
            "signal": str(row["signal"]),
            "reversal": bool(row["psar_reversal"]),
            "af": float(row["psar_af"]) if pd.notna(row["psar_af"]) else None,
        }
        for ts, row in daily_psar.iterrows()
        if pd.notna(row.get("psar"))
    ]
    daily_latest = None
    if not daily_psar.empty:
        daily_latest = {
            "signal": str(daily_psar["signal"].iloc[-1]),
            "psar": float(daily_psar["psar"].iloc[-1]) if pd.notna(daily_psar["psar"].iloc[-1]) else None,
            "reversal": bool(daily_psar["psar_reversal"].iloc[-1]),
        }

    return {
        "screener": "PSAR",
        "symbol": normalized,
        "name": summary_payload["name"],
        "sector": summary_payload["sector"],
        "signal": summary_payload["signal"],
        "signal_strength": summary_payload["signal_strength"],
        "price": summary_payload["price"],
        "change": summary_payload["change"],
        "change_pct": summary_payload["change_pct"],
        "stats": {
            "psar_1day": summary_payload["psar_1day"],
            "psar_1hour": summary_payload["psar_1hour"],
            "reversal_1day": summary_payload["reversal_1day"],
            "reversal_1hour": summary_payload["reversal_1hour"],
            "af": summary_payload["af"],
            "max_af": summary_payload["max_af"],
        },
        "timeframes": {
            "1day": {
                "candles": daily_candles,
                "psar": daily_psar_records,
                "latest": daily_latest,
            },
            "1hour": {
                "candles": hourly_records,
                "psar": hourly_psar_records,
                "latest": hourly_latest,
            },
        },
    }


def build_psar_snapshots_streaming(conn: Connection) -> Tuple[Dict[str, Any], int]:
    symbols, index_memberships, index_filters = load_index_memberships()
    if not symbols:
        raise ValueError("Index symbol lists missing")

    meta = load_symbol_meta(conn, symbols, index_memberships=index_memberships)
    stocks: List[Dict[str, Any]] = []
    stock_snapshots_written = 0
    empty_daily = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    empty_hourly = pd.DataFrame(columns=["ts", "open", "high", "low", "close"])
    stock_prefix = "strategy_screener:psar:stock:"

    for chunk_symbols in _iter_symbol_chunks(symbols, PSAR_CHUNK_SIZE):
        daily = read_daily_ohlc_for_symbols(conn, chunk_symbols, DETAIL_LOOKBACK_DAYS)
        hourly = read_hourly_ohlc_for_symbols(conn, chunk_symbols, PSAR_HOURLY_LOOKBACK_DAYS)
        daily_grouped = {str(sym).upper(): g for sym, g in daily.groupby("symbol", sort=False)}
        hourly_grouped = {str(sym).upper(): g for sym, g in hourly.groupby("symbol", sort=False)}

        for symbol in chunk_symbols:
            daily_frame = daily_grouped.get(symbol, empty_daily)
            hourly_frame = hourly_grouped.get(symbol, empty_hourly)
            frame_summary = daily_frame.tail(SUMMARY_LOOKBACK_DAYS) if not daily_frame.empty else daily_frame
            signal_payload = build_psar_signal_payload(symbol, frame_summary, hourly_frame, meta)
            stocks.append(signal_payload)

            if daily_frame.empty:
                continue
            detail_payload = build_psar_stock_detail_payload(symbol, daily_frame, hourly_frame, meta)
            upsert_snapshot(conn, f"{stock_prefix}{symbol}", detail_payload)
            stock_snapshots_written += 1

    buy_count = sum(1 for s in stocks if s["signal"] == "BUY")
    sell_count = sum(1 for s in stocks if s["signal"] == "SELL")
    neutral_count = sum(1 for s in stocks if s["signal"] == "NEUTRAL")
    stocks.sort(
        key=lambda x: (
            x["signal"] != "BUY",
            x["signal"] == "NEUTRAL",
            -float(x.get("signal_strength") or 0.0),
            x["symbol"],
        )
    )

    summary = {
        "screener": "PSAR",
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "counts": {
            "buy": buy_count,
            "sell": sell_count,
            "neutral": neutral_count,
            "total": len(stocks),
        },
        "index_filters": index_filters,
        "stocks": stocks,
    }
    return summary, stock_snapshots_written


def _build_super_breakout(conn: Connection) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    return build_super_breakout_snapshot_bundle(conn)


def _build_custom_dma(conn: Connection) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    return build_custom_dma_snapshot_bundle(conn)


def _build_psar(conn: Connection) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    # PSAR is generated via streaming path in run_snapshot_job for lower memory usage.
    return {"screener": "PSAR", "generated_at": pd.Timestamp.utcnow().isoformat(), "counts": {}, "stocks": []}, {}


SCREENER_BUILDERS: Dict[str, Callable[[Connection], Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]]] = {
    "super_breakout": _build_super_breakout,
    "custom_dma": _build_custom_dma,
    "psar": _build_psar,
}


def run_snapshot_job(engine: Engine, screener: str) -> None:
    started_at = utc_now()
    with engine.begin() as conn:
        create_snapshot_table(conn)

        if not try_acquire_snapshot_job_lock(conn):
            print("Snapshot job lock is active; skipping this run.")
            return

        try:
            builders: Dict[str, Callable[[Connection], Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]]]
            if screener == "all":
                builders = SCREENER_BUILDERS
            else:
                if screener not in SCREENER_BUILDERS:
                    raise ValueError(f"Unsupported screener '{screener}'.")
                builders = {screener: SCREENER_BUILDERS[screener]}

            upsert_snapshot(
                conn,
                "strategy_screener:catalog",
                {
                    "screeners": [
                        {
                            "id": "super-breakout",
                            "key": "super_breakout",
                            "name": "Super Breakout",
                            "description": "Trend following screener",
                        },
                        {
                            "id": "custom-dma",
                            "key": "custom_dma",
                            "name": "Custom DMA",
                            "description": "Moving average screener",
                        },
                        {
                            "id": "psar",
                            "key": "psar",
                            "name": "PSAR",
                            "description": "Momentum screener",
                        }
                    ],
                    "generated_at": utc_now().isoformat(),
                },
            )

            for screener_key, builder in builders.items():
                print(f"Building snapshots for {screener_key}...")
                summary_key = f"strategy_screener:{screener_key}:summary"
                stock_prefix = f"strategy_screener:{screener_key}:stock:"
                if screener_key == "super_breakout":
                    summary, stocks_snapshotted = build_super_breakout_snapshots_streaming(conn)
                    upsert_snapshot(conn, summary_key, summary)
                    detail_count = stocks_snapshotted
                elif screener_key == "custom_dma":
                    summary, stocks_snapshotted = build_custom_dma_snapshots_streaming(conn)
                    upsert_snapshot(conn, summary_key, summary)
                    detail_count = stocks_snapshotted
                elif screener_key == "psar":
                    summary, stocks_snapshotted = build_psar_snapshots_streaming(conn)
                    upsert_snapshot(conn, summary_key, summary)
                    detail_count = stocks_snapshotted
                else:
                    summary, details = builder(conn)
                    upsert_snapshot(conn, summary_key, summary)
                    for symbol, payload in details.items():
                        upsert_snapshot(conn, f"{stock_prefix}{symbol}", payload)
                    detail_count = len(details)

                upsert_snapshot(
                    conn,
                    f"strategy_screener:{screener_key}:meta",
                    {
                        "screener_key": screener_key,
                        "summary_key": summary_key,
                        "stock_snapshot_prefix": stock_prefix,
                        "stocks_snapshotted": detail_count,
                        "generated_at": utc_now().isoformat(),
                        "counts": summary.get("counts") or {},
                    },
                )
                print(f"Completed {screener_key}: summary + {detail_count} stock snapshots")

            upsert_snapshot(
                conn,
                "strategy_screener:job_meta",
                {
                    "job": "generate_strategy_screener_snapshots",
                    "started_at": started_at.isoformat(),
                    "finished_at": utc_now().isoformat(),
                    "screener": screener,
                    "status": "success",
                },
            )
        finally:
            try:
                release_snapshot_job_lock(conn)
            except PendingRollbackError:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    release_snapshot_job_lock(conn)
                except Exception as exc:
                    print(f"⚠️ Failed to release snapshot job lock after rollback: {exc}")
            except Exception as exc:
                try:
                    conn.rollback()
                    release_snapshot_job_lock(conn)
                except Exception:
                    pass
                print(f"⚠️ Failed to release snapshot job lock: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate strategy screener snapshots")
    parser.add_argument(
        "--screener",
        default="all",
        help="Screener key to generate (super_breakout, custom_dma, psar) or all",
    )
    parser.add_argument(
        "--db-url",
        default=DATABASE_URL,
        help="Postgres URL (defaults to DATABASE_URL env or built-in fallback)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = create_engine(args.db_url, pool_pre_ping=True)
    run_snapshot_job(engine, args.screener)
    print("Strategy screener snapshots refreshed.")


if __name__ == "__main__":
    main()
