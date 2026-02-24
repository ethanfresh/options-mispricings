# pricing.py

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq

# Black-Scholes Pricer
def bs_call(S, K, T, r, sigma):
    d1 = (np.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)

def bs_put(S, K, T, r, sigma):
    d1 = (np.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)

# Implied Volatility Solver
def implied_vol(price, S, K, T, r, option_type="call"):

    def objective(sigma):
        if option_type == "call":
            return bs_call(S, K, T, r, sigma) - price
        else:
            return bs_put(S, K, T, r, sigma) - price

    return brentq(objective, 1e-4, 5.0)

# Risk-Neutral PDF (Breeden-Litzenberger)
# Must use discounted call prices
# Must normalize
# Must enforce non-negativity
def implied_pdf(strikes, call_prices, r, T):

    strikes = np.array(strikes)
    call_prices = np.array(call_prices)

    pdf = np.zeros(len(strikes))

    for i in range(1, len(strikes)-1):

        dK1 = strikes[i] - strikes[i-1]
        dK2 = strikes[i+1] - strikes[i]

        first_deriv_forward = (call_prices[i+1] - call_prices[i]) / dK2
        first_deriv_backward = (call_prices[i] - call_prices[i-1]) / dK1

        second_deriv = (first_deriv_forward - first_deriv_backward) / ((dK1+dK2)/2)

        pdf[i] = np.exp(r*T) * second_deriv

    pdf = np.maximum(pdf, 0)
    pdf /= np.trapz(pdf, strikes)

    return pdf

# True ITM Probability
def true_itm_probability(strikes, pdf, strike):

    strikes = np.array(strikes)
    pdf = np.array(pdf)

    mask = strikes >= strike
    return np.trapz(pdf[mask], strikes[mask])

# Naive BS Probability
def naive_itm_probability(S, K, T, r, sigma):
    d2 = (np.log(S/K)+(r-0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    return norm.cdf(d2)

# Mispricing Metric
def probability_mispricing(prob_true, prob_naive):
    return prob_naive - prob_true

# Expected Value for a Call
def expected_value_call(strikes, pdf, strike, option_price):

    strikes = np.array(strikes)
    pdf = np.array(pdf)

    payoff = np.maximum(strikes - strike, 0)

    expected_payoff = np.trapz(payoff * pdf, strikes)

    return expected_payoff - option_price

# condor_ev = (
#    ev_short_call
#    - ev_long_call
#    + ev_short_put
#    - ev_long_put
# )
