"""
database.py

Creates and initializes the options.db SQLite database for storing
Massive (Polygon) options chain snapshot data.

This schema is optimized for:

- Fast historical queries
- Efficient storage
- Volatility surface reconstruction
- Mispricing detection
- Scalability to tens of millions of rows
"""

import sqlite3

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_FILE = os.path.join(BASE_DIR, "options.db")

def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Enable WAL mode for faster concurrent reads/writes
    cursor.execute("PRAGMA journal_mode=WAL;")

    # ============================
    # CONTRACTS TABLE
    # Static contract information
    # ============================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS contracts (

        contract_symbol TEXT PRIMARY KEY,

        underlying_symbol TEXT NOT NULL,

        contract_type TEXT NOT NULL,
        exercise_style TEXT NOT NULL,

        expiration_date TEXT NOT NULL,

        strike_price REAL NOT NULL,

        shares_per_contract INTEGER NOT NULL

    );
    """)

    # Index for fast expiration queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_contracts_expiration
    ON contracts(expiration_date);
    """)

    # Index for fast underlying queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_contracts_underlying
    ON contracts(underlying_symbol);
    """)


    # ============================
    # SNAPSHOTS TABLE
    # Time-series market data
    # ============================

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS snapshots (

        snapshot_time TEXT NOT NULL,

        contract_symbol TEXT NOT NULL,

        underlying_symbol TEXT NOT NULL,


        -- DAILY DATA
        day_change REAL,
        day_change_percent REAL,
        day_close REAL,
        day_high REAL,
        day_low REAL,
        day_open REAL,
        day_previous_close REAL,
        day_volume INTEGER,
        day_vwap REAL,
        day_last_updated_ns INTEGER NOT NULL,
        day_last_updated_utc TEXT,


        -- GREEKS
        delta REAL,
        gamma REAL,
        theta REAL,
        vega REAL,


        -- VOLATILITY
        implied_volatility REAL,


        -- OPEN INTEREST
        open_interest INTEGER,


        PRIMARY KEY (contract_symbol, day_last_updated_ns),
                   
        FOREIGN KEY (contract_symbol)
            REFERENCES contracts(contract_symbol)

    );
    """)

    # ============================
    # PERFORMANCE INDEXES
    # ============================

    # Fast time queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_snapshots_time
    ON snapshots(snapshot_time);
    """)

    # Fast contract queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_snapshots_contract
    ON snapshots(contract_symbol);
    """)

    # Fast underlying queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_snapshots_underlying
    ON snapshots(underlying_symbol);
    """)

    # Fast expiration joins
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_snapshots_contract_updated
    ON snapshots(contract_symbol, day_last_updated_ns);
    """)


    conn.commit()
    conn.close()

    print("Database initialized successfully.")
    print(f"Database file: {DB_FILE}")


if __name__ == "__main__":
    initialize_database()
