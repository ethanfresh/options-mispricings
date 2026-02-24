"""
fetcher.py

Fetches options chain snapshot data from Massive (Polygon) API
using Massive RESTClient (handles pagination automatically).

Returns structured data ready for insertion into options.db.

This module does NOT write to the database.
It only fetches and parses data.

Designed to work with database.py schema.
"""

from massive import RESTClient
from datetime import datetime, timezone


# ============================
# CONFIGURATION
# ============================

API_KEY = "m5GkXnwARzqdEwbw5DCzW9UdiNlFnpFt"

client = RESTClient(API_KEY)


# ============================
# FETCH FUNCTIONS
# ============================


def fetch_chain_raw(symbol: str):
    """
    Fetch full options chain using Massive RESTClient.
    Automatically handles pagination.
    """

    print(f"[FETCHER] Requesting options chain for {symbol}...")

    results = []

    try:

        for contract in client.list_snapshot_options_chain(symbol):

            results.append(contract)

        print(f"[FETCHER] Successfully fetched {len(results)} contracts for {symbol}")

        return results

    except Exception as e:

        print(f"[FETCHER ERROR] Failed to fetch data: {e}")

        return []


# ============================
# PARSING FUNCTIONS
# ============================


def parse_contract(o):
    """
    Extract contract table fields from Massive contract object.
    """

    return {

        "contract_symbol": o.details.ticker,

        "underlying_symbol": o.underlying_asset.ticker,

        "contract_type": o.details.contract_type,

        "exercise_style": o.details.exercise_style,

        "expiration_date": o.details.expiration_date,

        "strike_price": o.details.strike_price,

        "shares_per_contract": o.details.shares_per_contract,
    }


def parse_snapshot(o, snapshot_time: str):
    """
    Extract snapshot table fields from Massive contract object.
    """

    day = o.day
    greeks = o.greeks

    # Nanoseconds timestamp
    ns = day.last_updated if day else None

    # Convert to UTC ISO
    if ns is not None:
        utc = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat()
    else:
        utc = None

    return {

        "snapshot_time": snapshot_time,

        "contract_symbol": o.details.ticker,

        "underlying_symbol": o.underlying_asset.ticker,


        # Day data
        "day_change": day.change if day else None,
        "day_change_percent": day.change_percent if day else None,
        "day_close": day.close if day else None,
        "day_high": day.high if day else None,
        "day_low": day.low if day else None,
        "day_open": day.open if day else None,
        "day_previous_close": day.previous_close if day else None,
        "day_volume": day.volume if day else None,
        "day_vwap": day.vwap if day else None,

        "day_last_updated_ns": ns,
        "day_last_updated_utc": utc,


        # Greeks
        "delta": greeks.delta if greeks else None,
        "gamma": greeks.gamma if greeks else None,
        "theta": greeks.theta if greeks else None,
        "vega": greeks.vega if greeks else None,


        # Volatility
        "implied_volatility": o.implied_volatility,


        # Open interest
        "open_interest": o.open_interest,
    }


# ============================
# MAIN FETCH FUNCTION
# ============================


def fetch_chain(symbol: str):
    """
    Fetch and parse full options chain.

    Returns:
        contracts_list
        snapshots_list
    """

    raw_chain = fetch_chain_raw(symbol)

    if not raw_chain:

        print("[FETCHER] No contracts returned.")

        return [], []

    snapshot_time = datetime.now(timezone.utc).isoformat()

    contracts = []
    snapshots = []

    for o in raw_chain:

        contracts.append(parse_contract(o))

        snapshots.append(parse_snapshot(o, snapshot_time))

    print(f"[FETCHER] Parsed {len(contracts)} contracts successfully.")
    print(f"[FETCHER] Snapshot timestamp: {snapshot_time}")

    return contracts, snapshots


# ============================
# TEST EXECUTION
# ============================


if __name__ == "__main__":

    symbol = "NVDA"

    contracts, snapshots = fetch_chain(symbol)

    print("\n[FETCHER] TEST SUMMARY")
    print(f"Contracts parsed: {len(contracts)}")
    print(f"Snapshots parsed: {len(snapshots)}")

    if contracts:
        print("\nSample contract:")
        print(contracts[0])

    if snapshots:
        print("\nSample snapshot:")
        print(snapshots[0])

    print("\n[FETCHER] SUCCESS")
