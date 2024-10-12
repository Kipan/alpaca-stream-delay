import pandas as pd
import time
import logging
from alpaca.data import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta, timezone
from Stream import Stream
from IPython.display import display
from config import API_KEY, SECRET_KEY

class PoorStream(Stream):
    DELAY_MINUTES = 15

    def __init__(self, window, bar_length, symbol, exchange):
        super().__init__()
        self.window = window
        self.bar_length = pd.to_timedelta(bar_length)
        self.symbol = symbol
        self.exchange = exchange
        self.raw_data = None
        self.last_bar = None
        self.tick_data = pd.DataFrame()
        self.streamed_15 = False # we'll check this during the on_success method
        self.stock_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
        self.crypto_client = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)
        self.set_tickers({self.exchange:[self.symbol]})

    def create_timedelta(self, bar_length, window):
        if bar_length == pd.to_timedelta("1d"):
            return timedelta(days=window)
        elif bar_length == pd.to_timedelta("1h"):
            return timedelta(hours=window)
        elif bar_length == pd.to_timedelta("1min"):
            return timedelta(minutes=window)
        else:
            raise ValueError("Invalid unit. Use 'day', 'hour', or 'minute'.")
        
    async def on_recv(self, q):
        print('quote', q)
        recent_tick = q.timestamp.replace(tzinfo=None)
        df = pd.DataFrame({self.symbol:q.close}, index = [recent_tick])
        #df = pd.DataFrame({self.symbol:q.close}, 
        #                  index = [recent_tick])
        self.tick_data = pd.concat([self.tick_data, df])
        if self.streamed_15:
            self.resample_and_join()
            display(self.raw_data)

        if not self.streamed_15 and recent_tick - self.start \
            >= timedelta(minutes=self.DELAY_MINUTES):
            self.streamed_15 = True
            self.get_most_recent()
            print("its been {} minutes".format(self.DELAY_MINUTES))
            display(self.raw_data)

    def resample_and_join(self):
        self.raw_data = pd.concat([self.raw_data, self.tick_data])#.resample(self.bar_length, 
                                                                 #       label="right").last().ffill().iloc[:-1]])
        self.tick_data = pd.DataFrame()#self.tick_data.iloc[-1:]
        self.last_bar = self.raw_data.index[-1]

    def get_most_recent(self):
        #while True:
        time.sleep(2)
        #now = datetime.now(timezone.utc).replace(tzinfo=None)
        #now = now - timedelta(microseconds = now.microsecond) - timedelta(minutes=self.DELAY_MINUTES)
        past = self.start - self.create_timedelta(self.bar_length, self.window)
        if self.exchange == 'crypto':
            request_params = CryptoBarsRequest(
                symbol_or_symbols = self.symbol,
                timeframe = TimeFrame.Minute,
                start = past,
                end = self.start
            )
            df = self.crypto_client.get_crypto_bars(request_params=request_params).df
        elif self.exchange == 'stock':
            request_params = StockBarsRequest(
                symbol_or_symbols = self.symbol,
                timeframe = TimeFrame.Minute,
                start = past,
                end = self.start,
                adjustment = "all"
            )
            df = self.stock_client.get_stock_bars(request_params=request_params).df
        df = df.reset_index()
        df = df[["timestamp", "close"]]
        df = df.set_index("timestamp")
        df.rename(columns = {"close":self.symbol}, inplace = True)
        df = df.resample(self.bar_length, label = "right").last().dropna().iloc[:-1]
        self.raw_data = df.copy()
        self.last_bar = self.raw_data.index[-1]
            #if pd.to_datetime(datetime.now(timezone.utc).replace(tzinfo=None)).tz_localize("UTC") - self.last_bar - \
            #    pd.to_timedelta("{}m".format(self.DELAY_MINUTES)) < self.bar_length:
            #    break

    def run(self):
        self.start = datetime.now(timezone.utc).replace(tzinfo=None)
        super().run()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    ps = PoorStream(window=10, bar_length="1m", symbol="ETH/USD", exchange="crypto")
    ps.run()