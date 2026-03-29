from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests, io
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

# ---------------- CONFIG ----------------
DB_URL = "postgresql://postgres:zKvzg4nrnigizkpbc1fN@budhi.cjwye68sytlr.ap-south-1.rds.amazonaws.com:5432/postgres"

IST = ZoneInfo("Asia/Kolkata")

END_DATE = datetime.now(IST).date()
START_DATE = END_DATE - timedelta(days=7)
# ---------------------------------------

engine = create_engine(DB_URL)

print("Run time IST:", datetime.now(IST))
print("Processing:", START_DATE, "→", END_DATE)

# ---------- NSE SESSION ----------
session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/"
}

# mandatory cookie warm-up
session.get(
    "https://www.nseindia.com/api/marketStatus",
    headers=headers,
    timeout=10
)

inserted_days = 0
failed_days = []

d = START_DATE
while d <= END_DATE:

    date_str = d.strftime("%d-%b-%Y")

    try:

        # ============================================================
        # FII DERIVATIVE STATISTICS (XLS)
        # ============================================================

        fii_url = f"https://archives.nseindia.com/content/fo/fii_stats_{date_str}.xls"

        r = session.get(fii_url, headers=headers, timeout=10)
        r.raise_for_status()

        raw = pd.read_excel(io.BytesIO(r.content), header=None, engine="xlrd")

        raw = raw.dropna(how="all")

        raw = raw[
            ~raw[0].astype(str).str.contains(
                "Notes|Both buy|Options Value|Futures Value|Value & Open Interest",
                na=False
            )
        ]

        # identify segment rows
        raw["segment"] = None
        raw.loc[raw[0].str.contains("FUTURES|OPTIONS", na=False), "segment"] = raw[0]
        raw["segment"] = raw["segment"].ffill()

        # actual numeric rows
        data = raw[
            raw[1].apply(
                lambda x: str(x).replace(".", "").isdigit()
                if pd.notna(x)
                else False
            )
        ]

        if data.empty:
            raise ValueError("FII stats empty")

        data = data.rename(columns={
            0: "instrument",
            1: "buy_contracts",
            2: "buy_value",
            3: "sell_contracts",
            4: "sell_value",
            5: "oi_contracts",
            6: "oi_value"
        })

        data["date"] = d
        data["segment"] = raw["segment"].loc[data.index].values

        data = data[
            [
                "date",
                "segment",
                "instrument",
                "buy_contracts",
                "buy_value",
                "sell_contracts",
                "sell_value",
                "oi_contracts",
                "oi_value"
            ]
        ]

        # ---------- INSERT (IDEMPOTENT) ----------
        with engine.begin() as conn:

            meta = MetaData()

            fii_table = Table(
                "fii_derivatives_stats",
                meta,
                autoload_with=conn
            )

            stmt = insert(fii_table).values(data.to_dict("records"))

            stmt = stmt.on_conflict_do_nothing(
                index_elements=["date", "segment", "instrument"]
            )

            conn.execute(stmt)

        inserted_days += 1
        print(f"✅ {date_str} FII stats inserted")

    except Exception as e:

        failed_days.append((date_str, str(e)))
        print(f"❌ {date_str} failed → {e}")

    d += timedelta(days=1)

print("\n==============================")
print(f"✅ Days processed: {inserted_days}")
print(f"❌ Days failed: {len(failed_days)}")

if failed_days:
    for f in failed_days:
        print("FAILED:", f)
