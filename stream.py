import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.enums import DataFeed
from datetime import timedelta, datetime, timezone
import pandas as pd
from config import API_KEY, SECRET_KEY

class Stream():
    def __init__(self):
        self.stock_conn = StockDataStream(API_KEY, SECRET_KEY)
        self.crypto_conn = CryptoDataStream(API_KEY, SECRET_KEY)


    async def on_recv(self, q):
        print('quote', q)



    def add_stream(self, exchange, ticker):
        if exchange == 'stock':
            conn = self.stock_conn
        elif exchange == 'crypto':
            conn = self.crypto_conn
        else:
            print("exchange must be stock or crypto")
            return

        try:
            self.consumer_thread(conn, ticker)
        except KeyboardInterrupt:
            print("Interrupted execution by user")
            conn.stop_ws()
            exit(0)
        except Exception as e:
            print("You got an exception: {} during execution. continue "
                  "execution.".format(e))
            # let the execution continue


    def consumer_thread(self, conn, ticker):

        conn.subscribe_bars(self.on_recv, ticker)
        #conn.run()

    def set_tickers(self, dict: dict):
        # dict follows {'crypto':[ticker], 'stock':[ticker]}
        for exchange, item_arr in dict.items():
            for ticker in item_arr:
                self.add_stream(exchange, ticker)
    

    def run(self):
        self.crypto_conn.run()
        self.stock_conn.run()

    async def stop(self):
        await self.crypto_conn.close()
        await self.stock_conn.close()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    str = Stream()
    str.set_tickers({
        'crypto': ['BTC/USD', 'ETH/USD', 'ETH/BTC']
    })
    str.run()
    print("hhhhhhhhhhhhhhhhhhhhhh")
    time.sleep(5)  # give the initial connection time to be established