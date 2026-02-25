"""
collector.py

Fetches options chain data using fetcher.py and stores it in options.db.

Responsibilities:
- Calls fetch_chain() from fetcher.py
- Inserts new contracts into contracts table
- Inserts snapshot records into snapshots table
- Avoids duplicate contract entries
- Avoids duplicate snapshots
- Prints clear success messages
"""

import sqlite3
from datetime import datetime, timezone

from fetcher import fetch_chain

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_FILE = os.path.join(BASE_DIR, "options.db")

# ============================
# DATABASE INSERT FUNCTIONS
# ============================

def insert_contracts(cursor, contracts):
    """
    Insert contracts into contracts table.
    Uses INSERT OR IGNORE to prevent duplicates.
    """

    sql = """
    INSERT OR IGNORE INTO contracts (
        contract_symbol,
        underlying_symbol,
        contract_type,
        exercise_style,
        expiration_date,
        strike_price,
        shares_per_contract
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    data = [
        (
            c["contract_symbol"],
            c["underlying_symbol"],
            c["contract_type"],
            c["exercise_style"],
            c["expiration_date"],
            c["strike_price"],
            c["shares_per_contract"],
        )
        for c in contracts
    ]

    cursor.executemany(sql, data)

    return cursor.rowcount


def insert_snapshots(cursor, snapshots):
    """
    Insert snapshot records into snapshots table.
    Uses INSERT OR IGNORE to prevent duplicate snapshot entries.
    """

    sql = """
    INSERT OR IGNORE INTO snapshots (
        snapshot_time,
        contract_symbol,
        underlying_symbol,

        day_change,
        day_change_percent,
        day_close,
        day_high,
        day_low,
        day_open,
        day_previous_close,
        day_volume,
        day_vwap,

        day_last_updated_ns,
        day_last_updated_utc,

        delta,
        gamma,
        theta,
        vega,

        implied_volatility,
        open_interest
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    data = [
        (
            s["snapshot_time"],
            s["contract_symbol"],
            s["underlying_symbol"],

            s["day_change"],
            s["day_change_percent"],
            s["day_close"],
            s["day_high"],
            s["day_low"],
            s["day_open"],
            s["day_previous_close"],
            s["day_volume"],
            s["day_vwap"],

            s["day_last_updated_ns"],
            s["day_last_updated_utc"],

            s["delta"],
            s["gamma"],
            s["theta"],
            s["vega"],

            s["implied_volatility"],
            s["open_interest"],
        )
        for s in snapshots
    ]

    cursor.executemany(sql, data)

    return cursor.rowcount


# ============================
# MAIN COLLECTION FUNCTION
# ============================

def collect(symbol: str):

    print(f"\n[COLLECTOR] Starting collection for {symbol}")

    start_time = datetime.now(timezone.utc)

    # Fetch data
    contracts, snapshots = fetch_chain(symbol)

    if not contracts or not snapshots:
        print("[COLLECTOR] No data received. Aborting.")
        return

    # Connect to database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:

        print("[COLLECTOR] Inserting contracts...")
        contract_count = insert_contracts(cursor, contracts)

        print(f"[COLLECTOR] Contracts inserted: {contract_count}")

        print("[COLLECTOR] Inserting snapshots...")
        snapshot_count = insert_snapshots(cursor, snapshots)

        print(f"[COLLECTOR] Snapshots inserted: {snapshot_count}")

        conn.commit()

        end_time = datetime.now(timezone.utc)

        duration = (end_time - start_time).total_seconds()

        print(f"[COLLECTOR] SUCCESS")
        print(f"[COLLECTOR] Symbol: {symbol}")
        print(f"[COLLECTOR] Contracts processed: {len(contracts)}")
        print(f"[COLLECTOR] Snapshots processed: {len(snapshots)}")
        print(f"[COLLECTOR] Duration: {duration:.2f} seconds")

    except Exception as e:

        conn.rollback()

        print(f"[COLLECTOR ERROR] {e}")

    finally:

        conn.close()


# ============================
# SCRIPT ENTRY POINT
# ============================

if __name__ == "__main__":

    SYMBOLS = [
        "NVDA",
        # Add more symbols here later
        "SPY",
        "AAPL",
        "TSLA",
        "QQQ"
    ]

    print("[COLLECTOR] Starting batch collection")

    for symbol in SYMBOLS:
        collect(symbol)

    print("\n[COLLECTOR] All collections complete")