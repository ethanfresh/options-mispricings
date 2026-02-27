import numpy as np
import pandas as pd
from scipy.stats import norm
from datetime import datetime

def find_all_iron_condors(
    underlying_symbol: str,
    date: str,
    expiration_date: str,
    risk_free_rate: float,
    conn
) -> pd.DataFrame:
    """
    Iron condor generator using Black–Scholes d2 probability model.
    """

    query = """
    SELECT 
        c.contract_symbol,
        c.contract_type,
        c.strike_price,
        s.day_close,
        s.day_vwap,
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

    # Filter bad IV
    df = df[(df.implied_volatility > 0.05) & (df.implied_volatility < 3)]

    if df.empty:
        return pd.DataFrame()

    # Spot price (same for all)
    S = df.day_close.iloc[0]

    # Time to expiration (in years)
    T = (
        datetime.strptime(expiration_date, "%Y-%m-%d")
        - datetime.strptime(date, "%Y-%m-%d")
    ).days / 365.0

    if T <= 0:
        return pd.DataFrame()

    puts = df[df.contract_type == "put"].sort_values("strike_price").reset_index(drop=True)
    calls = df[df.contract_type == "call"].sort_values("strike_price").reset_index(drop=True)

    if puts.empty or calls.empty:
        return pd.DataFrame()

    puts = puts.fillna(0)
    calls = calls.fillna(0)

    # Convert to arrays
    pK = puts.strike_price.values
    pPrice = puts.day_vwap.values
    pIV = puts.implied_volatility.values
    pOI = puts.open_interest.values

    cK = calls.strike_price.values
    cPrice = calls.day_vwap.values
    cIV = calls.implied_volatility.values
    cOI = calls.open_interest.values

    # ------------------------
    # PUT SPREADS
    # ------------------------

    sellK_p = pK[:, None]
    buyK_p  = pK[None, :]

    width_p  = sellK_p - buyK_p
    credit_p = pPrice[:, None] - pPrice[None, :]

    valid_put = (buyK_p < sellK_p) & (credit_p > 0)

    i_put, j_put = np.where(valid_put)

    put_width  = width_p[i_put, j_put]
    put_credit = credit_p[i_put, j_put]
    put_sell_strike = pK[i_put]
    put_sell_iv     = pIV[i_put]
    put_sell_oi     = pOI[i_put]
    put_buy_strike = pK[j_put]

    put_delta = -puts.delta.values[i_put] + puts.delta.values[j_put]
    put_gamma = -puts.gamma.values[i_put] + puts.gamma.values[j_put]
    put_theta = -puts.theta.values[i_put] + puts.theta.values[j_put]
    put_vega  = -puts.vega.values[i_put]  + puts.vega.values[j_put]

    # ------------------------
    # CALL SPREADS
    # ------------------------

    sellK_c = cK[:, None]
    buyK_c  = cK[None, :]

    width_c  = buyK_c - sellK_c
    credit_c = cPrice[:, None] - cPrice[None, :]

    valid_call = (buyK_c > sellK_c) & (credit_c > 0)

    i_call, j_call = np.where(valid_call)

    call_width  = width_c[i_call, j_call]
    call_credit = credit_c[i_call, j_call]
    call_sell_strike = cK[i_call]
    call_sell_iv     = cIV[i_call]
    call_sell_oi     = cOI[i_call]
    call_buy_strike = cK[j_call]

    call_delta = -calls.delta.values[i_call] + calls.delta.values[j_call]
    call_gamma = -calls.gamma.values[i_call] + calls.gamma.values[j_call]
    call_theta = -calls.theta.values[i_call] + calls.theta.values[j_call]
    call_vega  = -calls.vega.values[i_call]  + calls.vega.values[j_call]

    # ------------------------
    # COMBINE INTO CONDORS
    # ------------------------

    put_strike = put_sell_strike[:, None]
    call_strike = call_sell_strike[None, :]

    valid_condor = call_strike > put_strike

    net_credit = put_credit[:, None] + call_credit[None, :]

    put_max_loss = put_width[:, None] - put_credit[:, None]
    call_max_loss = call_width[None, :] - call_credit[None, :]

    max_risk = np.maximum(put_max_loss, call_max_loss)

    net_delta = put_delta[:, None] + call_delta[None, :]
    net_gamma = put_gamma[:, None] + call_gamma[None, :]
    net_theta = put_theta[:, None] + call_theta[None, :]
    net_vega  = put_vega[:, None]  + call_vega[None, :]

    # ------------------------
    # BLACK–SCHOLES POP (Strike Specific IV)
    # ------------------------

    sigma_put  = put_sell_iv[:, None]
    sigma_call = call_sell_iv[None, :]

    # Avoid divide-by-zero
    sigma_put  = np.maximum(sigma_put, 1e-6)
    sigma_call = np.maximum(sigma_call, 1e-6)

    sqrtT = np.sqrt(T)

    d2_put = (
        np.log(S / put_strike)
        + (risk_free_rate - 0.5 * sigma_put**2) * T
    ) / (sigma_put * sqrtT)

    d2_call = (
        np.log(S / call_strike)
        + (risk_free_rate - 0.5 * sigma_call**2) * T
    ) / (sigma_call * sqrtT)

    sigma_put_2d  = np.broadcast_to(sigma_put,  net_credit.shape)
    sigma_call_2d = np.broadcast_to(sigma_call, net_credit.shape)

    # Probability price ends between strikes
    pop = norm.cdf(d2_call) - norm.cdf(d2_put)

    pop = np.clip(pop, 0, 1)

    expected_value = pop * net_credit - (1 - pop) * max_risk

    liquidity = np.minimum(
        put_sell_oi[:, None],
        call_sell_oi[None, :]
    )

    result = pd.DataFrame({
        "expected_value": expected_value[valid_condor],
        "pop": pop[valid_condor],
        "net_credit": net_credit[valid_condor],
        "max_risk": max_risk[valid_condor],
        "return_on_risk": net_credit[valid_condor] / max_risk[valid_condor],
        "put_iv": sigma_put_2d[valid_condor],
        "call_iv": sigma_call_2d[valid_condor],
        "liquidity": liquidity[valid_condor],
        "net_delta": net_delta[valid_condor],
        "net_gamma": net_gamma[valid_condor],
        "net_theta": net_theta[valid_condor],
        "net_vega": net_vega[valid_condor],
    })

    return result.sort_values(
        "expected_value",
        ascending=False
    ).reset_index(drop=True)