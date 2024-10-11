# alpaca-stream-delay
### Disclaimer: use at your own risk
if you're poor like me and don't have Alpaca's Algo Trader Plus subscription, this will help you get around the 15 minute historical data limitation

Alpaca limits historical data to the last 15 minutes which might impact your trading strategies while you have a websocket and are missing 15 min of data. The PoorStream class in stream_delay.py will help get around that by delaying retrieval of bars for 15 minutes.
