from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests, io
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
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

# ---------- ENSURE TABLE ----------
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fao_participant_oi (
            date DATE NOT NULL,
            participant_type TEXT NOT NULL,
            future_long BIGINT,
            future_short BIGINT,
            option_call_long BIGINT,
            option_call_short BIGINT,
            option_put_long BIGINT,
            option_put_short BIGINT,
            PRIMARY KEY (date, participant_type)
        );
    """))

# ---------- NSE SESSION ----------
session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com/"
}

# cookie warmup
session.get(
    "https://www.nseindia.com/api/marketStatus",
    headers=headers,
    timeout=10
)

inserted_days = 0
failed_days = []

d = START_DATE

while d <= END_DATE:

    ds = d.strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ds}.csv"

    try:

        r = session.get(url, headers=headers, timeout=10)

        if r.status_code == 404:
            print(f"⚠️ {d} OI not published")
            d += timedelta(days=1)
            continue

        r.raise_for_status()

        raw = pd.read_csv(io.StringIO(r.text), header=None)
        raw = raw.dropna(how="all")

        # --- find first participant row ---
        start_idx = None

        for i in range(len(raw)):
            val = str(raw.iloc[i, 0]).strip().upper()

            if val in ("CLIENT", "FII", "DII", "PRO"):
                start_idx = i
                break

        if start_idx is None:
            raise ValueError("No participant rows found")

        data = raw.iloc[start_idx:start_idx + 4].copy()
        data.columns = range(data.shape[1])

        rows = []

        for _, r in data.iterrows():

            rows.append({
                "date": d,
                "participant_type": r[0],
                "future_long": r[1] + r[3],
                "future_short": r[2] + r[4],
                "option_call_long": r[5] + r[9],
                "option_call_short": r[7] + r[11],
                "option_put_long": r[6] + r[10],
                "option_put_short": r[8] + r[12],
            })

        final = pd.DataFrame(rows)

        # ---------- INSERT ----------
        with engine.begin() as conn:

            meta = MetaData()

            table = Table(
                "fao_participant_oi",
                meta,
                autoload_with=conn
            )

            stmt = insert(table).values(final.to_dict("records"))

            stmt = stmt.on_conflict_do_nothing(
                index_elements=["date", "participant_type"]
            )

            conn.execute(stmt)

        inserted_days += 1
        print(f"✅ {d} OI inserted")

    except Exception as e:

        failed_days.append((d, str(e)))
        print(f"❌ {d} failed → {e}")

    d += timedelta(days=1)

print("\n==============================")
print(f"✅ Days processed: {inserted_days}")
print(f"❌ Days failed: {len(failed_days)}")

if failed_days:
    for f in failed_days:
        print("FAILED:", f)
