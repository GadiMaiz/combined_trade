import time
from threading import Thread
import logging

class OrderbookBase:
    SPREAD_MOVING_AVERAGE_INTERVAL_SEC = 0.1
    ORDERBOOK_HEALTH_INTERVAL_SEC = 1
    ORDERBOOK_HEALTH_TIMEOUT_SEC = 20
    ORDERBOOK_HEALTH_COMPARE_LENGTH = 4
    SPREAD_MINIMUM_SAMPLES_FOR_MOVING = 100

    def __init__(self, asset_pairs):
        self._calculate_orderbook_thread = None
        self._orderbook_running = False
        self._average_spreads = {}
        self._spread_samples = {}
        self._last_trade = {}
        self._asset_pairs = asset_pairs
        self._log = logging.getLogger(__name__)
        for curr_asset_pair in self.get_assets_pair():
            self._average_spreads[curr_asset_pair] = 0
            self._spread_samples[curr_asset_pair] = 0

    def start_orderbook(self):
        if self._calculate_orderbook_thread is None or not self._calculate_orderbook_thread.is_alive():
            self._calculate_orderbook_thread = Thread(target=self._calculate_orderbook_params,
                                                      daemon=True,
                                                      name='Calculate Orderbook Thread')
            self._orderbook_running = True
            self._calculate_orderbook_thread.start()
        started = False
        while not started:
            try:
                self._start()
                started = True
            except Exception as e:
                self._log.error("Error starting orderbook: <%s>, retrying", e)
                time.sleep(1)


    def stop_orderbook(self):
        self._orderbook_running = False
        self._stop()

    def _start(self):
        print("start")

    def _stop(self):
        print("stop")

    def get_current_partial_book(self, asset_pair, size):
        result = {'asks': [],
                  'bids': []}
        return result

    def _calculate_orderbook_params(self):
        curr_time = time.time()
        prev_average_time = 0
        prev_orderbooks_compare_time = 0
        different_orderbooks_timestamp = curr_time
        compare_orderbook = {}
        for curr_asset_pair in self._asset_pairs:
            self._spread_samples[curr_asset_pair] = 0
            compare_orderbook[curr_asset_pair] = self.get_current_partial_book(curr_asset_pair,
                                                                               OrderbookBase.ORDERBOOK_HEALTH_COMPARE_LENGTH)


        while self._orderbook_running:
            for curr_asset_pair in self.get_assets_pair():
                if curr_time - prev_average_time >= OrderbookBase.SPREAD_MOVING_AVERAGE_INTERVAL_SEC:
                    curr_spread = self.get_current_spread(curr_asset_pair)
                    if curr_spread > 0:
                        self._spread_samples[curr_asset_pair] += 1
                        spread_ratio = (1/min(self._spread_samples[curr_asset_pair], OrderbookBase.SPREAD_MINIMUM_SAMPLES_FOR_MOVING))
                        self._average_spreads[curr_asset_pair] = (1 - spread_ratio) * self._average_spreads[curr_asset_pair] + \
                                                                 spread_ratio * curr_spread

                """if curr_time - prev_orderbooks_compare_time >= OrderbookBase.ORDERBOOK_HEALTH_INTERVAL_SEC:
                    current_compare_orderbook = self.get_current_partial_book(curr_asset_pair,
                                                                              OrderbookBase.ORDERBOOK_HEALTH_COMPARE_LENGTH)
                    if len(current_compare_orderbook['asks']) != len(compare_orderbook[curr_asset_pair]['asks']) or \
                        len(current_compare_orderbook['bids']) != len(compare_orderbook[curr_asset_pair]['bids']):
                        compare_orderbook[curr_asset_pair] = current_compare_orderbook
                    else:
                        identical_orderbooks = True
                        for curr_type in current_compare_orderbook:
                            if not identical_orderbooks:
                                break
                            compare_index = 0
                            while compare_index < len(current_compare_orderbook[curr_type]) and identical_orderbooks:
                                if current_compare_orderbook[curr_type][compare_index]['price'] != \
                                        compare_orderbook[curr_asset_pair][curr_type][compare_index]['price'] or \
                                    current_compare_orderbook[curr_type][compare_index]['size'] != \
                                        compare_orderbook[curr_asset_pair][curr_type][compare_index]['size']:
                                    identical_orderbooks = False
                                compare_index += 1
                        if not identical_orderbooks:
                            different_orderbooks_timestamp = time.time()
                        elif identical_orderbooks and different_orderbooks_timestamp - time.time() > OrderbookBase.ORDERBOOK_HEALTH_TIMEOUT_SEC:
                            print("Orderbook hasn't change for more than",OrderbookBase.ORDERBOOK_HEALTH_TIMEOUT_SEC, "seconds, restarting it")
                            self._calculate_orderbook_thread = None
                            self._stop()
                            self._start()
                    prev_orderbooks_compare_time = curr_time"""

            prev_average_time = curr_time
            curr_time = time.time()
            time.sleep(min(OrderbookBase.SPREAD_MOVING_AVERAGE_INTERVAL_SEC,
                           OrderbookBase.ORDERBOOK_HEALTH_INTERVAL_SEC))

    def get_average_spread(self, asset_pair):
        return self._average_spreads[asset_pair]

    def get_current_spread_and_price(self, asset_pair):
        curr_spread = 0
        curr_price = self.get_current_price(asset_pair)
        if curr_price['ask'] is not None and curr_price['bid'] is not None:
            curr_spread = curr_price['ask'] - curr_price['bid']

        curr_price['spread'] = curr_spread
        return curr_price

    def get_current_spread(self, asset_pair):
        return self.get_current_spread_and_price(asset_pair)['spread']

    def get_current_price(self, asset_pair):
        curr_price = {'ask' : None, 'bid' : None}
        curr_orders = self.get_current_partial_book(asset_pair, 1)
        if curr_orders is not None and len(curr_orders['asks']) > 0 and len(curr_orders['bids']) > 0:
            curr_price['ask'] = curr_orders['asks'][0]['price']
            curr_price['bid'] = curr_orders['bids'][0]['price']

        return curr_price

    def is_orderbook_thread_alive(self):
        return False

    def get_last(self, pair):
        if pair in self._last_trade:
            return self._last_trade[pair]
        else:
            return None

    def is_thread_orderbook(self):
        return True

    def get_assets_pair(self):
        return self._asset_pairs