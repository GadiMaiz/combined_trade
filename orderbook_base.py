import time
from threading import Thread
import logging
from execution_rate_tracker import ExecutionRateTracker
from enum import Enum


class OrderbookFee(Enum):
    NO_FEE = 0
    TAKER_FEE = 1
    MAKER_FEE = 2

class OrderbookBase:
    SPREAD_MOVING_AVERAGE_INTERVAL_SEC = 0.1
    ORDERBOOK_HEALTH_INTERVAL_SEC = 1
    ORDERBOOK_HEALTH_TIMEOUT_SEC = 20
    ORDERBOOK_HEALTH_COMPARE_LENGTH = 4
    SPREAD_MINIMUM_SAMPLES_FOR_MOVING = 100
    LIVE_TRADES_MAXIMUM_AGE_SEC = 600

    def __init__(self, asset_pairs, fees):
        self._calculate_orderbook_thread = None
        self._orderbook_running = False
        self._average_spreads = {}
        self._spread_samples = {}
        self._last_trade = {}
        self._asset_pairs = asset_pairs
        self._live_trades = {}
        self._rate_trackers = {}
        self._fees = None
        self.set_fees(fees)
        for curr_asset_pair in self._asset_pairs:
            self._live_trades[curr_asset_pair] = []
            self._rate_trackers[curr_asset_pair] = {'buy': ExecutionRateTracker(),
                                                    'sell': ExecutionRateTracker()}
        self._log = logging.getLogger(__name__)
        self._orders_for_listening = {}

        for curr_asset_pair in self.get_asset_pairs():
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

    def _get_orderbook_from_exchange(self, asset_pair, size):
        result = {'asks': [],
                  'bids': []}
        return result

    def get_current_partial_book(self, asset_pair, size, include_fees_in_price):
        partial_orderbook = self._get_orderbook_from_exchange(asset_pair, size)
        if include_fees_in_price != OrderbookFee.NO_FEE:
            fees = self.get_fees()
            change_fee = 0
            if include_fees_in_price == OrderbookFee.MAKER_FEE:
                change_fee = fees['make']
            elif include_fees_in_price == OrderbookFee.TAKER_FEE:
                change_fee = fees['take']
            partial_orderbook['asks_with_fee'] = list(partial_orderbook['asks'])
            partial_orderbook['bids_with_fee'] = list(partial_orderbook['bids'])
            self._set_fee_to_orders(partial_orderbook['asks_with_fee'], change_fee)
            self._set_fee_to_orders(partial_orderbook['bids_with_fee'], change_fee * -1)
        return partial_orderbook

    @staticmethod
    def _set_fee_to_orders(orders, fee):
        for order_index in range(len(orders)):
            orders[order_index]['price'] *= (1 + 0.01 * fee)

    def _calculate_orderbook_params(self):
        curr_time = time.time()
        prev_average_time = 0
        prev_orderbooks_compare_time = 0
        different_orderbooks_timestamp = curr_time
        """compare_orderbook = {}
        for curr_asset_pair in self._asset_pairs:
            self._spread_samples[curr_asset_pair] = 0
            compare_orderbook[curr_asset_pair] = self.get_current_partial_book(curr_asset_pair,
                                                                               OrderbookBase.ORDERBOOK_HEALTH_COMPARE_LENGTH)"""
        while self._orderbook_running:
            for curr_asset_pair in self.get_asset_pairs():
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
        curr_price = self.get_current_price(asset_pair)
        if curr_price:
            curr_price['spread'] = curr_price['ask']['price'] - curr_price['bid']['price']
        return curr_price

    def get_current_spread(self, asset_pair):
        curr_spread = 0
        curr_price_and_spread = self.get_current_spread_and_price(asset_pair)
        if curr_price_and_spread:
            curr_spread = curr_price_and_spread['spread']
        return curr_spread

    def get_current_price(self, asset_pair, include_fee=OrderbookFee.NO_FEE):
        curr_price = None
        curr_orders = self.get_current_partial_book(asset_pair, 1, include_fee)
        if curr_orders is not None and len(curr_orders['asks']) > 0 and len(curr_orders['bids']) > 0:
            curr_price = {'ask': {'price': curr_orders['asks'][0]['price'], 'size': curr_orders['asks'][0]['size']},
                          'bid': {'price': curr_orders['bids'][0]['price'], 'size': curr_orders['bids'][0]['size']}}
            if include_fee != OrderbookFee.NO_FEE:
                curr_price['ask_with_fee'] = {'price': curr_orders['asks_with_fee'][0]['price']}
                curr_price['bid_with_fee'] = {'price': curr_orders['bids_with_fee'][0]['price']}

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

    def get_asset_pairs(self):
        return self._asset_pairs

    def get_exchange_rate(self, crypto_type, price):
        asset_pair = crypto_type + "-USD"
        live_trades_size = len(self._live_trades[asset_pair])

    def listen_for_order(self, order_id, order_listener):
        self._orders_for_listening[order_id] = order_listener

    def stop_listening_for_order(self, order_id):
        self._orders_for_listening.pop(order_id, None)

    def get_tracked_info(self, asset_pair):
        return {'buy_price': self._rate_trackers[asset_pair]['buy'].get_price(),
                'buy_size': self._rate_trackers[asset_pair]['buy'].get_size(),
                'sell_price': self._rate_trackers[asset_pair]['sell'].get_price(),
                'sell_size': self._rate_trackers[asset_pair]['sell'].get_size()}

    def get_fees(self):
        return self._fees

    def set_fees(self, fees):
        self._fees = fees

    def add_fees_to_partial_orderbook(self, partial_orderbook):
        fees = self.get_fees()

