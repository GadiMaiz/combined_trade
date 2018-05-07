import time
from threading import Thread

class OrderbookBase:
    SPREAD_MOVING_AVERAGE_INTERVAL_SEC = 0.1
    SPREAD_MINIMUM_SAMPLES_FOR_MOVING = 100

    def __init__(self, asset_pairs):
        self._spread_thread = None
        self._spread_running = False
        self._average_spreads = {}
        self._spread_samples = {}
        self._asset_pairs = asset_pairs
        for curr_asset_pair in self._asset_pairs:
            self._average_spreads[curr_asset_pair] = 0
            self._spread_samples[curr_asset_pair] = 0

    def start_orderbook(self):
        if self._spread_thread is None or not self._spread_thread.is_alive():
            self._spread_thread = Thread(target=self._calculate_spread,
                                         daemon=True,
                                         name='Calculate Spread Thread')
            self._spread_running = True
            self._spread_thread.start()
        self._start()

    def stop_orderbook(self):
        self._spread_running = False
        self._stop()

    def _start(self):
        print ("start")

    def _stop(self):
        print ("stop")

    def get_current_partial_book(self, asset_pair, size):
        result = {'asks': [],
                  'bids': []}
        return result

    def _calculate_spread(self):
        for curr_asset_pair in self._asset_pairs:
            self._spread_samples[curr_asset_pair] = 0

        while self._spread_running:

            for curr_asset_pair in self._asset_pairs:
                curr_spread = self.get_current_spread(curr_asset_pair)

                if curr_spread > 0:
                    self._spread_samples[curr_asset_pair] += 1
                    spread_ratio = (1/min(self._spread_samples[curr_asset_pair], OrderbookBase.SPREAD_MINIMUM_SAMPLES_FOR_MOVING))
                    self._average_spreads[curr_asset_pair] = (1 - spread_ratio) * self._average_spreads[curr_asset_pair] + \
                                                             spread_ratio * curr_spread
            time.sleep(OrderbookBase.SPREAD_MOVING_AVERAGE_INTERVAL_SEC)

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
