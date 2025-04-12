import ccxt
import pandas as pd
import time

# Initialize the exchange (Binance in this case)
exchange = ccxt.binance({
    'enableRateLimit': True  # to avoid being rate-limited by the exchange
})

# Define parameters
symbol = 'BTC/USDT'
timeframe = '1m'
limit = 1000  # max candles per fetch
since = exchange.parse8601('2017-10-01T00:00:00Z')  # starting point

# Container for the data
all_candles = []

# Fetch in batches
while True:
    candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
    if not candles:
        break
    all_candles += candles
    print(f"Fetched {len(candles)} candles, total: {len(all_candles)}")
    
    # Move to next batch
    since = candles[-1][0] + 1

    # Sleep to respect rate limits
    time.sleep(exchange.rateLimit / 1000)

# Convert to DataFrame
df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df = df.drop(columns=['open', 'high', 'low'])

# Save to CSV
df.to_csv('btc_m.csv', index=False)

print("Saved to btc_m.csv")

