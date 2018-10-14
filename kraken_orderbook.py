import json
from orderbook_base import OrderbookBase
from bitex.api.REST import KrakenREST
import time
from threading import Thread, Lock
import logging

class KrakenOrderbook(OrderbookBase):
    MINIMUM_REFRESH_INTERVAL_SEC = 10
    KRAKEN_PAIRS_DICT = {'BTC-USD': 'XXBTZUSD', 'BCH-USD': 'BCHUSD', 'BTC-EUR': 'XXBTZEUR', 'BCH-EUR': 'BCHEUR',
                         'LTC-EUR': 'XLTCZEUR', 'BCH-BTC': 'BCHXBT', 'LTC-BTC': 'XLTCXXBT', 'ETH-BTC': 'XETHXXBT'}

    def __init__(self, asset_pairs, fees, **kwargs):
        super().__init__(asset_pairs, fees)
        self._last_orderbooks = dict()
        self._last_orderbook_timestamp = dict()
        self._last_trades_timestamp = dict()
        self._orderbook_mutex = Lock()
        self._log = logging.getLogger(__name__)

    def _start(self):
        pass

    def _stop(self):
        pass

    def _get_orderbook_from_exchange(self, asset_pair, size):
        kraken_pair = asset_pair
        if asset_pair in KrakenOrderbook.KRAKEN_PAIRS_DICT:
            kraken_pair = KrakenOrderbook.KRAKEN_PAIRS_DICT[asset_pair]

        curr_time = time.time()
        orders = None
        self._orderbook_mutex.acquire()
        try:
            if kraken_pair in self._last_orderbooks and kraken_pair in self._last_orderbook_timestamp and curr_time - \
                    self._last_orderbook_timestamp[kraken_pair] <= KrakenOrderbook.MINIMUM_REFRESH_INTERVAL_SEC:
                orders = self._last_orderbooks[kraken_pair]
            else:
                try:
                    k = KrakenREST()
                    orders_bytes = k.query('GET', 'public/Depth', params={'pair': kraken_pair})
                    orders = json.loads(orders_bytes.content)
                    self._last_orderbook_timestamp[kraken_pair] = curr_time
                    self._last_orderbooks[kraken_pair] = orders
                except Exception as e:
                    self._log.error("Kraken exception:", e)
                    if kraken_pair in self._last_orderbooks[kraken_pair]:
                        orders = self._last_orderbooks[kraken_pair]
                    else:
                        orders = {"result": {kraken_pair: {'asks': [], 'bids': []}}}
        finally:
            self._orderbook_mutex.release()
        result = {
            'asks': [],
            'bids': [],
        }

        for i in range(min(size, len(orders["result"][kraken_pair]["asks"]))):
            result['asks'].append({"price": float(orders["result"][kraken_pair]["asks"][i][0]),
                                   "size": float(orders["result"][kraken_pair]["asks"][i][1]),
                                   'source': "Kraken"})

        for i in range(min(size, len(orders["result"][kraken_pair]["bids"]))):
            result['bids'].append({"price": float(orders["result"][kraken_pair]["bids"][i][0]),
                                   "size": float(orders["result"][kraken_pair]["bids"][i][1]),
                                   'source': "Kraken"})

        #print("Get Kraken book:", asset_pair, size, result)
        return result

    def get_last(self, pair):
        result = None
        if pair in KrakenOrderbook.KRAKEN_PAIRS_DICT:
            kraken_pair = KrakenOrderbook.KRAKEN_PAIRS_DICT[pair]
            curr_time = time.time()
            if kraken_pair not in self._last_trades_timestamp or curr_time - self._last_trades_timestamp[kraken_pair] > \
                KrakenOrderbook.MINIMUM_REFRESH_INTERVAL_SEC:
                k = KrakenREST()
                try:
                    trades_bytes = k.query('GET', 'public/Trades', params={'pair': kraken_pair})
                    trades = json.loads(trades_bytes.content)
                    if 'result' in trades and kraken_pair in trades['result']:
                        last_trade = trades['result'][kraken_pair][len(trades['result'][kraken_pair]) - 1]
                        type = "buy"
                        if last_trade[3] == 's':
                            type = "sell"
                        self._last_trade[kraken_pair] = {'price': float(last_trade[0]),
                                                         'type': type,
                                                         'time': last_trade[2]}
                except Exception as e:
                    print("Kraken orderbook exception:", e)
                    if kraken_pair not in self._last_trades_timestamp:
                        self._last_trade[kraken_pair] = {'price': 0,
                                                         'type': "",
                                                         'time': time.time()}
            result = self._last_trade[kraken_pair]
        return result
