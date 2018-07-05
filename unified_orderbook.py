import heapq
import operator
from threading import Thread, Lock
from orderbook_base import OrderbookFee


class UnifiedOrderbook:
    def __init__(self, orderbooks):
        self._orderbooks = orderbooks
        self._is_thread_orderbook = False
        self._orders_mutex = Lock()

    def set_orderbook(self, exchange, orderbook):
        try:
            self._orders_mutex.acquire()
            if orderbook:
                self._orderbooks[exchange] = orderbook
            else:
                self._orderbooks.pop(exchange, None)
        finally:
            self._orders_mutex.release()

    def get_unified_orderbook(self, symbol, size, include_fees_in_price):
        client_orderbooks = []
        try:
            self._orders_mutex.acquire()
            for curr_orderbook in self._orderbooks:
                client_orderbooks.append(self._orderbooks[curr_orderbook].get_current_partial_book(
                    symbol, size, include_fees_in_price))
            best_orders = {'asks': [], 'bids': []}
            price_sort = 'price'
            order_keys = [[heapq.nsmallest, 'asks'], [heapq.nlargest, 'bids']]
            if include_fees_in_price != OrderbookFee.NO_FEE:
                price_sort = 'price_with_fee'
            for curr_orderbook in client_orderbooks:
                for curr_keyset in order_keys:
                    best_orders[curr_keyset[1]] = curr_keyset[0](size, best_orders[curr_keyset[1]] +
                                                                 curr_orderbook[curr_keyset[1]],
                                                                 key=operator.itemgetter(price_sort))
        finally:
            self._orders_mutex.release()

        return best_orders

    def is_thread_orderbook(self):
        return False

    def get_current_spread_and_price(self, asset_pair):
        best_orders = self.get_unified_orderbook(asset_pair, 1, False)
        spread_and_price = {'ask': {'price': 0}, 'bid': {'price': 0}}
        if len(best_orders['asks']) > 0:
            spread_and_price['ask']['price'] = best_orders['asks'][0]['price']
        if len(best_orders['bids']) > 0:
            spread_and_price['bid']['price'] = best_orders['bids'][0]['price']
        return spread_and_price

    def get_average_spread(self, asset_pair):
        return 0

    def get_fees(self):
        return {'make': 0, 'take': 0}
