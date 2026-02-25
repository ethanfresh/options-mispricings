import pandas as pd
import numpy as np

def find_all_iron_condors(underlying_symbol, date, expiration_date, conn):
    """
    Find all possible iron condors for a given date and return as DataFrame.
    
    Parameters:
    - underlying_symbol: e.g., 'NVDA'
    - date: The snapshot date to analyze (format: 'YYYY-MM-DD')
    - expiration_date: Target expiration date for the options
    - conn: SQLite database connection
    
    Returns:
    - DataFrame with all possible iron condor combinations
    """
    
    # Get all options for this underlying on the given date with the target expiration
    query = """
    SELECT 
        c.contract_symbol,
        c.contract_type,
        c.strike_price,
        c.expiration_date,
        s.day_vwap as price,
        s.implied_volatility,
        s.delta,
        s.gamma,
        s.theta,
        s.vega,
        s.open_interest,
        s.snapshot_time
    FROM contracts c
    JOIN snapshots s ON c.contract_symbol = s.contract_symbol
    WHERE c.underlying_symbol = ?
        AND c.expiration_date = ?
        AND DATE(s.snapshot_time) = ?
        AND s.day_close IS NOT NULL
    ORDER BY c.contract_type, c.strike_price
    """
    
    df = pd.read_sql_query(query, conn, params=(underlying_symbol, expiration_date, date))
    
    if df.empty:
        return pd.DataFrame()
    
    # Separate calls and puts
    puts = df[df['contract_type'] == 'put'].sort_values('strike_price')
    calls = df[df['contract_type'] == 'call'].sort_values('strike_price')
    
    condors = []
    
    # Iterate through all possible iron condor combinations
    for i, put_sell in puts.iterrows():
        for j, put_buy in puts.iterrows():
            if put_buy['strike_price'] >= put_sell['strike_price']:
                continue
                
            for k, call_sell in calls.iterrows():
                if call_sell['strike_price'] <= put_sell['strike_price']:
                    continue
                    
                for m, call_buy in calls.iterrows():
                    if call_buy['strike_price'] <= call_sell['strike_price']:
                        continue
                    
                    # Calculate net credit (sell premium - buy premium)
                    net_credit = (put_sell['price'] + call_sell['price'] - 
                                 put_buy['price'] - call_buy['price'])
                    
                    # Calculate max risk
                    put_width = put_sell['strike_price'] - put_buy['strike_price']
                    call_width = call_buy['strike_price'] - call_sell['strike_price']
                    max_risk = max(put_width, call_width) - net_credit
                    
                    # Calculate net Greeks
                    net_delta = (
                        -call_sell['delta'] if call_sell['delta'] is not None else 0
                        + call_buy['delta'] if call_buy['delta'] is not None else 0
                        - put_sell['delta'] if put_sell['delta'] is not None else 0
                        + put_buy['delta'] if put_buy['delta'] is not None else 0
                    )
                    
                    net_gamma = (
                        -call_sell['gamma'] if call_sell['gamma'] is not None else 0
                        + call_buy['gamma'] if call_buy['gamma'] is not None else 0
                        - put_sell['gamma'] if put_sell['gamma'] is not None else 0
                        + put_buy['gamma'] if put_buy['gamma'] is not None else 0
                    )
                    
                    net_theta = (
                        -call_sell['theta'] if call_sell['theta'] is not None else 0
                        + call_buy['theta'] if call_buy['theta'] is not None else 0
                        - put_sell['theta'] if put_sell['theta'] is not None else 0
                        + put_buy['theta'] if put_buy['theta'] is not None else 0
                    )
                    
                    net_vega = (
                        -call_sell['vega'] if call_sell['vega'] is not None else 0
                        + call_buy['vega'] if call_buy['vega'] is not None else 0
                        - put_sell['vega'] if put_sell['vega'] is not None else 0
                        + put_buy['vega'] if put_buy['vega'] is not None else 0
                    )
                    
                    # Calculate condor IV (vega-weighted average)
                    legs = [
                        {'vega': put_buy['vega'], 'implied_volatility': put_buy['implied_volatility'], 'open_interest': put_buy['open_interest']},
                        {'vega': put_sell['vega'], 'implied_volatility': put_sell['implied_volatility'], 'open_interest': put_sell['open_interest']},
                        {'vega': call_sell['vega'], 'implied_volatility': call_sell['implied_volatility'], 'open_interest': call_sell['open_interest']},
                        {'vega': call_buy['vega'], 'implied_volatility': call_buy['implied_volatility'], 'open_interest': call_buy['open_interest']}
                    ]
                    
                    # Filter out legs with None values for IV calculation
                    valid_legs = [leg for leg in legs if leg['vega'] is not None and leg['implied_volatility'] is not None]
                    
                    if valid_legs:
                        total_vega = sum(abs(leg['vega']) for leg in valid_legs)
                        condor_iv = sum(abs(leg['vega']) * leg['implied_volatility'] for leg in valid_legs) / total_vega if total_vega > 0 else None
                    else:
                        condor_iv = None
                    
                    # Calculate liquidity score
                    open_interests = [leg['open_interest'] for leg in legs if leg['open_interest'] is not None]
                    liquidity = min(open_interests) if open_interests else None
                    
                    # Calculate probability of profit
                    if call_sell['delta'] is not None and put_sell['delta'] is not None:
                        pop = (1 - abs(call_sell['delta'])) * (1 - abs(put_sell['delta']))
                    else:
                        pop = None
                    
                    # Calculate expected value
                    if pop is not None:
                        expected_value = pop * net_credit - (1 - pop) * max_risk
                    else:
                        expected_value = None
                    
                    condors.append({
                        'expected_value': expected_value,
                        'pop': pop,
                        'net_theta': net_theta,
                        'return_on_risk': round((net_credit / max_risk * 100) if max_risk > 0 else 0, 2),
                        'net_vega': net_vega,
                        'liquidity': liquidity,
                        'net_delta': net_delta,
                        'condor_iv': condor_iv,
                        'net_credit': net_credit,
                        'max_risk': max_risk,
                        'net_gamma': net_gamma,
                        'put_buy_strike': put_buy['strike_price'],
                        'put_buy_price': put_buy['price'],
                        'put_sell_strike': put_sell['strike_price'],
                        'put_sell_price': put_sell['price'],
                        'call_sell_strike': call_sell['strike_price'],
                        'call_sell_price': call_sell['price'],
                        'call_buy_strike': call_buy['strike_price'],
                        'call_buy_price': call_buy['price'],
                        'put_buy_symbol': put_buy['contract_symbol'],
                        'put_sell_symbol': put_sell['contract_symbol'],
                        'call_sell_symbol': call_sell['contract_symbol'],
                        'call_buy_symbol': call_buy['contract_symbol']






                    })
    
    return pd.DataFrame(condors)

def find_all_iron_condors_fast(underlying_symbol, date, expiration_date, conn):

    query = """
    SELECT 
        c.contract_symbol,
        c.contract_type,
        c.strike_price,
        s.day_vwap as price,
        s.implied_volatility,
        s.delta,
        s.gamma,
        s.theta,
        s.vega,
        s.open_interest
    FROM contracts c
    JOIN snapshots s ON c.contract_symbol = s.contract_symbol
    WHERE c.underlying_symbol = ?
        AND c.expiration_date = ?
        AND DATE(s.snapshot_time) = ?
        AND s.day_close IS NOT NULL
    ORDER BY c.contract_type, c.strike_price
    """

    df = pd.read_sql_query(query, conn, params=(underlying_symbol, expiration_date, date))

    if df.empty:
        return pd.DataFrame()

    puts = df[df.contract_type == "put"].reset_index(drop=True)
    calls = df[df.contract_type == "call"].reset_index(drop=True)

    puts = puts.sort_values("strike_price").reset_index(drop=True)
    calls = calls.sort_values("strike_price").reset_index(drop=True)

    # Convert to numpy arrays (VERY FAST)
    p_strikes = puts.strike_price.values
    p_price = puts.price.values
    p_delta = puts.delta.fillna(0).values
    p_gamma = puts.gamma.fillna(0).values
    p_theta = puts.theta.fillna(0).values
    p_vega = puts.vega.fillna(0).values

    c_strikes = calls.strike_price.values
    c_price = calls.price.values
    c_delta = calls.delta.fillna(0).values
    c_gamma = calls.gamma.fillna(0).values
    c_theta = calls.theta.fillna(0).values
    c_vega = calls.vega.fillna(0).values

    condors = []

    # Precompute put spreads
    put_spreads = []
    for sell in range(len(puts)):
        for buy in range(sell):
            width = p_strikes[sell] - p_strikes[buy]

            put_spreads.append({
                "sell_idx": sell,
                "buy_idx": buy,
                "credit": p_price[sell] - p_price[buy],
                "width": width,
                "delta": -p_delta[sell] + p_delta[buy],
                "gamma": -p_gamma[sell] + p_gamma[buy],
                "theta": -p_theta[sell] + p_theta[buy],
                "vega": -p_vega[sell] + p_vega[buy],
                "sell_strike": p_strikes[sell]
            })

    # Precompute call spreads
    call_spreads = []
    for sell in range(len(calls)):
        for buy in range(sell+1, len(calls)):

            width = c_strikes[buy] - c_strikes[sell]

            call_spreads.append({
                "sell_idx": sell,
                "buy_idx": buy,
                "credit": c_price[sell] - c_price[buy],
                "width": width,
                "delta": -c_delta[sell] + c_delta[buy],
                "gamma": -c_gamma[sell] + c_gamma[buy],
                "theta": -c_theta[sell] + c_theta[buy],
                "vega": -c_vega[sell] + c_vega[buy],
                "sell_strike": c_strikes[sell]
            })

    # Combine spreads into condors
    for ps in put_spreads:
        for cs in call_spreads:

            # ensure no overlap
            if cs["sell_strike"] <= ps["sell_strike"]:
                continue

            net_credit = ps["credit"] + cs["credit"]

            max_risk = max(ps["width"], cs["width"]) - net_credit

            if max_risk <= 0:
                continue

            net_delta = ps["delta"] + cs["delta"]
            net_gamma = ps["gamma"] + cs["gamma"]
            net_theta = ps["theta"] + cs["theta"]
            net_vega = ps["vega"] + cs["vega"]

            pop = (1 - abs(c_delta[cs["sell_idx"]])) * (1 - abs(p_delta[ps["sell_idx"]]))

            expected_value = pop * net_credit - (1-pop) * max_risk

            condors.append({
                "expected_value": expected_value,
                "pop": pop,
                "net_credit": net_credit,
                "max_risk": max_risk,
                "return_on_risk": net_credit/max_risk,
                "net_theta": net_theta,
                "net_vega": net_vega,
                "net_delta": net_delta,
                "net_gamma": net_gamma
            })

    return pd.DataFrame(condors)

def find_all_iron_condors_numpy(
    underlying_symbol: str,
    date: str,
    expiration_date: str,
    conn
) -> pd.DataFrame:
    """
    Fully vectorized iron condor generator.

    Returns DataFrame with all valid iron condors and key metrics.
    """

    query = """
    SELECT 
        c.contract_symbol,
        c.contract_type,
        c.strike_price,
        s.day_vwap AS price,
        s.implied_volatility,
        s.delta,
        s.gamma,
        s.theta,
        s.vega,
        s.open_interest
    FROM contracts c
    JOIN snapshots s ON c.contract_symbol = s.contract_symbol
    WHERE c.underlying_symbol = ?
        AND c.expiration_date = ?
        AND DATE(s.snapshot_time) = ?
        AND s.day_close IS NOT NULL
    ORDER BY c.contract_type, c.strike_price
    """

    df = pd.read_sql_query(
        query,
        conn,
        params=(underlying_symbol, expiration_date, date)
    )

    if df.empty:
        return pd.DataFrame()

    # Separate puts and calls
    puts = (
        df[df.contract_type == "put"]
        .sort_values("strike_price")
        .reset_index(drop=True)
    )

    calls = (
        df[df.contract_type == "call"]
        .sort_values("strike_price")
        .reset_index(drop=True)
    )

    if puts.empty or calls.empty:
        return pd.DataFrame()

    # Fill NaNs for safe vector math
    puts = puts.fillna(0)
    calls = calls.fillna(0)

    # Convert to NumPy arrays
    p_strike = puts.strike_price.values
    p_price  = puts.price.values
    p_delta  = puts.delta.values
    p_gamma  = puts.gamma.values
    p_theta  = puts.theta.values
    p_vega   = puts.vega.values
    p_iv     = puts.implied_volatility.values
    p_oi     = puts.open_interest.values

    c_strike = calls.strike_price.values
    c_price  = calls.price.values
    c_delta  = calls.delta.values
    c_gamma  = calls.gamma.values
    c_theta  = calls.theta.values
    c_vega   = calls.vega.values
    c_iv     = calls.implied_volatility.values
    c_oi     = calls.open_interest.values

    # --------------------------
    # Build PUT SPREADS
    # --------------------------

    ps_sell_strike = p_strike[:, None]
    ps_buy_strike  = p_strike[None, :]

    valid_put = ps_buy_strike < ps_sell_strike

    put_width  = ps_sell_strike - ps_buy_strike
    put_credit = p_price[:, None] - p_price[None, :]

    put_delta = -p_delta[:, None] + p_delta[None, :]
    put_gamma = -p_gamma[:, None] + p_gamma[None, :]
    put_theta = -p_theta[:, None] + p_theta[None, :]
    put_vega  = -p_vega[:, None] + p_vega[None, :]

    # Broadcast to full 2D shape for proper indexing
    put_sell_strike_2d = np.broadcast_to(p_strike[:, None], (len(p_strike), len(p_strike)))
    put_sell_iv_2d = np.broadcast_to(p_iv[:, None], (len(p_strike), len(p_strike)))
    put_sell_oi_2d = np.broadcast_to(p_oi[:, None], (len(p_strike), len(p_strike)))
    put_sell_delta_2d = np.broadcast_to(p_delta[:, None], (len(p_strike), len(p_strike)))

    put_spreads = pd.DataFrame({
        "put_width": put_width[valid_put],
        "put_credit": put_credit[valid_put],
        "put_delta": put_delta[valid_put],
        "put_gamma": put_gamma[valid_put],
        "put_theta": put_theta[valid_put],
        "put_vega": put_vega[valid_put],
        "put_sell_strike": put_sell_strike_2d[valid_put],
        "put_sell_iv": put_sell_iv_2d[valid_put],
        "put_sell_oi": put_sell_oi_2d[valid_put],
        "put_sell_delta": put_sell_delta_2d[valid_put],
    })

    # --------------------------
    # Build CALL SPREADS
    # --------------------------

    cs_sell_strike = c_strike[:, None]
    cs_buy_strike  = c_strike[None, :]

    valid_call = cs_buy_strike > cs_sell_strike

    call_width  = cs_buy_strike - cs_sell_strike
    call_credit = c_price[:, None] - c_price[None, :]

    call_delta = -c_delta[:, None] + c_delta[None, :]
    call_gamma = -c_gamma[:, None] + c_gamma[None, :]
    call_theta = -c_theta[:, None] + c_theta[None, :]
    call_vega  = -c_vega[:, None] + c_vega[None, :]

    # Broadcast to full 2D shape for proper indexing
    call_sell_strike_2d = np.broadcast_to(c_strike[:, None], (len(c_strike), len(c_strike)))
    call_sell_iv_2d = np.broadcast_to(c_iv[:, None], (len(c_strike), len(c_strike)))
    call_sell_oi_2d = np.broadcast_to(c_oi[:, None], (len(c_strike), len(c_strike)))
    call_sell_delta_2d = np.broadcast_to(c_delta[:, None], (len(c_strike), len(c_strike)))

    call_spreads = pd.DataFrame({
        "call_width": call_width[valid_call],
        "call_credit": call_credit[valid_call],
        "call_delta": call_delta[valid_call],
        "call_gamma": call_gamma[valid_call],
        "call_theta": call_theta[valid_call],
        "call_vega": call_vega[valid_call],
        "call_sell_strike": call_sell_strike_2d[valid_call],
        "call_sell_iv": call_sell_iv_2d[valid_call],
        "call_sell_oi": call_sell_oi_2d[valid_call],
        "call_sell_delta": call_sell_delta_2d[valid_call],
    })

    if put_spreads.empty or call_spreads.empty:
        return pd.DataFrame()

    # --------------------------
    # Combine into CONDORS
    # --------------------------

    ps = put_spreads
    cs = call_spreads

    ps_strike = ps.put_sell_strike.values[:, None]
    cs_strike = cs.call_sell_strike.values[None, :]

    valid_condor = cs_strike > ps_strike

    net_credit = (
        ps.put_credit.values[:, None]
        + cs.call_credit.values[None, :]
    )

    max_width = np.maximum(
        ps.put_width.values[:, None],
        cs.call_width.values[None, :]
    )

    max_risk = max_width - net_credit

    net_delta = ps.put_delta.values[:, None] + cs.call_delta.values[None, :]
    net_gamma = ps.put_gamma.values[:, None] + cs.call_gamma.values[None, :]
    net_theta = ps.put_theta.values[:, None] + cs.call_theta.values[None, :]
    net_vega  = ps.put_vega.values[:, None]  + cs.call_vega.values[None, :]

    condor_iv = (
        np.abs(ps.put_vega.values[:, None]) * ps.put_sell_iv.values[:, None]
        + np.abs(cs.call_vega.values[None, :]) * cs.call_sell_iv.values[None, :]
    ) / (
        np.abs(ps.put_vega.values[:, None])
        + np.abs(cs.call_vega.values[None, :])
        + 1e-9
    )

    liquidity = np.minimum(
        ps.put_sell_oi.values[:, None],
        cs.call_sell_oi.values[None, :]
    )

    pop = (
        (1 - np.abs(ps.put_sell_delta.values[:, None]))
        * (1 - np.abs(cs.call_sell_delta.values[None, :]))
    )

    expected_value = pop * net_credit - (1 - pop) * max_risk

    result = pd.DataFrame({
        "expected_value": expected_value[valid_condor],
        "pop": pop[valid_condor],
        "net_credit": net_credit[valid_condor],
        "max_risk": max_risk[valid_condor],
        "return_on_risk": net_credit[valid_condor] / max_risk[valid_condor],
        "net_delta": net_delta[valid_condor],
        "net_gamma": net_gamma[valid_condor],
        "net_theta": net_theta[valid_condor],
        "net_vega": net_vega[valid_condor],
        "condor_iv": condor_iv[valid_condor],
        "liquidity": liquidity[valid_condor],
    })

    return result.sort_values(
        "expected_value",
        ascending=False
    ).reset_index(drop=True)