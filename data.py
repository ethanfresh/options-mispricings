import requests
import pandas as pd

def fetch_option_chain(symbol):
    url = f"https://api.polygon.io/v3/snapshot/options/{symbol}?apiKey=m5GkXnwARzqdEwbw5DCzW9UdiNlFnpFt"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    

    options = []

    for contract in data["results"]:
        options.append({
            "strike": contract["details"]["strike_price"],
            "type": contract["details"]["contract_type"],
            "price": (contract["last_quote"]["bid"] +
                      contract["last_quote"]["ask"]) / 2
        })

    return pd.DataFrame(options)

print(fetch_option_chain('NVDA'))
