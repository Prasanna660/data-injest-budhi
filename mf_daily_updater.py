import logging
import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from psycopg2.extras import execute_values

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("mf_daily_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MF_FAST_DAILY")

# ================= DATABASE =================
DATABASE_URL = "postgresql+psycopg2://postgres:zKvzg4nrnigizkpbc1fN@budhi.cjwye68sytlr.ap-south-1.rds.amazonaws.com:5432/postgres"

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
    future=True
)

# ================= DOWNLOAD AMFI MASTER NAV =================
logger.info("Downloading AMFI NAVAll file...")

url = "https://www.amfiindia.com/spages/NAVAll.txt"
response = requests.get(url, timeout=60)

if response.status_code != 200:
    raise Exception("Failed to download NAVAll.txt")

lines = response.text.splitlines()

records = []
fund_house = None

for line in lines:
    if ";" not in line:
        fund_house = line.strip()
        continue

    parts = line.split(";")
    if len(parts) < 6:
        continue

    try:
        scheme_code = int(parts[0])
        scheme_name = parts[3]
        nav = float(parts[4])
        date = datetime.strptime(parts[5], "%d-%b-%Y").date()

        records.append({
            "scheme_code": scheme_code,
            "scheme_name": scheme_name,
            "fund_house": fund_house,
            "nav": nav,
            "nav_date": date
        })
    except:
        continue

logger.info(f"NAV records parsed: {len(records)}")

# ================= BULK INSERT =================
def insert_latest(conn, rows):
    sql = """
    INSERT INTO mf_latest_nav (scheme_code, nav, nav_date)
    VALUES %s
    ON CONFLICT (scheme_code)
    DO UPDATE SET
        nav = EXCLUDED.nav,
        nav_date = EXCLUDED.nav_date
    """

    values = [(r["scheme_code"], r["nav"], r["nav_date"]) for r in rows]

    raw = conn.connection
    with raw.cursor() as cur:
        execute_values(cur, sql, values, page_size=5000)

def insert_history(conn, rows):
    sql = """
    INSERT INTO mf_nav_history (scheme_code, nav_date, nav)
    VALUES %s
    ON CONFLICT (scheme_code, nav_date) DO NOTHING
    """

    values = [(r["scheme_code"], r["nav_date"], r["nav"]) for r in rows]

    raw = conn.connection
    with raw.cursor() as cur:
        execute_values(cur, sql, values, page_size=5000)

logger.info("Updating database...")

with engine.begin() as conn:
    insert_latest(conn, records)
    insert_history(conn, records)

logger.info("DAILY NAV UPDATE COMPLETE ✅")
