import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Any, Dict

from db.session import get_db
from utils.performance import cached_get_or_set, run_limited

router = APIRouter(prefix="/super-breakouts", tags=["super-breakouts"])

ATR_PERIOD = 10
ATR_MULTIPLIER = 3.0
BREAKOUT_WINDOW = {"left": 3, "right": 3}


def resample_ohlc(df: pd.DataFrame, timeframe: str):
    rule = {"weekly": "W-FRI", "monthly": "ME"}[timeframe]
    return (
        df.resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
    )


def compute_supertrend(df: pd.DataFrame, atr_period: int = ATR_PERIOD, multiplier: float = ATR_MULTIPLIER):
    df = df.copy()

    df["prev_close"] = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["prev_close"]).abs(),
            (df["low"] - df["prev_close"]).abs(),
        ],
        axis=1,
    ).max(axis=1)

    df["atr"] = tr.rolling(atr_period).mean()

    hl2 = (df["high"] + df["low"]) / 2
    df["upperband"] = hl2 + multiplier * df["atr"]
    df["lowerband"] = hl2 - multiplier * df["atr"]

    final_upper = []
    final_lower = []
    for i in range(len(df)):
        if i == 0:
            final_upper.append(df["upperband"].iloc[i])
            final_lower.append(df["lowerband"].iloc[i])
        else:
            final_upper.append(
                min(df["upperband"].iloc[i], final_upper[i - 1])
                if df["close"].iloc[i - 1] <= final_upper[i - 1]
                else df["upperband"].iloc[i]
            )
            final_lower.append(
                max(df["lowerband"].iloc[i], final_lower[i - 1])
                if df["close"].iloc[i - 1] >= final_lower[i - 1]
                else df["lowerband"].iloc[i]
            )

    df["final_upper"] = final_upper
    df["final_lower"] = final_lower

    supertrend = []
    direction = []
    for i in range(len(df)):
        if i == 0:
            supertrend.append(np.nan)
            direction.append(np.nan)
        else:
            if df["close"].iloc[i] > df["final_upper"].iloc[i - 1]:
                supertrend.append(df["final_lower"].iloc[i])
                direction.append(1)
            elif df["close"].iloc[i] < df["final_lower"].iloc[i - 1]:
                supertrend.append(df["final_upper"].iloc[i])
                direction.append(-1)
            else:
                supertrend.append(supertrend[i - 1])
                direction.append(direction[i - 1])

    df["supertrend"] = supertrend
    df["direction"] = direction
    return df.dropna(subset=["supertrend", "direction"])


def detect_breakouts(df: pd.DataFrame):
    breakouts = []
    for i in range(1, len(df)):
        prev_dir = df.iloc[i - 1]["direction"]
        curr_dir = df.iloc[i]["direction"]
        if prev_dir != curr_dir:
            breakouts.append(
                {
                    "ts": df.index[i],
                    "direction_from": int(prev_dir),
                    "direction_to": int(curr_dir),
                    "window": BREAKOUT_WINDOW,
                }
            )
    return breakouts


def _serialize_records_with_ts(records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in records:
        item = dict(row)
        ts_value = item.get("ts")
        if ts_value is not None:
            item["ts"] = pd.to_datetime(ts_value).isoformat()
        out.append(item)
    return out


def build_super_breakouts_payload(symbol: str, daily_df: pd.DataFrame) -> Dict[str, Any]:
    if daily_df.empty:
        raise HTTPException(404, f"No data found for {symbol}")

    daily = daily_df.copy()
    daily["ts"] = pd.to_datetime(daily["ts"])
    daily = daily.set_index("ts")

    result: Dict[str, Any] = {"symbol": symbol.upper(), "timeframes": {}}

    daily_st = compute_supertrend(daily)
    result["timeframes"]["daily"] = {
        "candles": _serialize_records_with_ts(daily.reset_index().to_dict("records")),
        "supertrend": _serialize_records_with_ts(daily_st[["supertrend", "direction"]].reset_index().to_dict("records")),
        "breakouts": _serialize_records_with_ts(detect_breakouts(daily_st)),
    }

    for tf in ["weekly", "monthly"]:
        resampled = resample_ohlc(daily, tf)
        st = compute_supertrend(resampled)
        result["timeframes"][tf] = {
            "candles": _serialize_records_with_ts(resampled.reset_index().to_dict("records")),
            "supertrend": _serialize_records_with_ts(st[["supertrend", "direction"]].reset_index().to_dict("records")),
            "breakouts": _serialize_records_with_ts(detect_breakouts(st)),
        }

    return result


def fetch_super_breakouts_payload(symbol: str, db_bind) -> Dict[str, Any]:
    query = text(
        """
        SELECT ts, open, high, low, close
        FROM ohlcv
        WHERE tradingsymbol = :sym
          AND interval = '1day'
        ORDER BY ts
        """
    )
    daily = pd.read_sql(query, db_bind, params={"sym": symbol})
    return build_super_breakouts_payload(symbol, daily)


@router.get("/{symbol}")
def get_super_breakouts(symbol: str, db: Session = Depends(get_db)):
    cache_key = f"super_breakouts:{symbol.upper()}"

    def _compute():
        return fetch_super_breakouts_payload(symbol, db.bind)

    return run_limited("super_breakouts", 2, lambda: cached_get_or_set(cache_key, 120, _compute))


@router.get("/health")
def health():
    return {"status": "healthy", "service": "super-breakouts-api"}
