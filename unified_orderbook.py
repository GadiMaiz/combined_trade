from bitex.api.WSS.bitstamp import BitstampWSS
from bitfinex_orderbook import BitfinexOrderbook
from queue import PriorityQueue
import heapq
import operator

class UnifiedOrderbook:
    def __init__(self, orderbooks):
        self._orderbooks = orderbooks

    def set_orderbook(self, exchange, orderbook):
        self._orderbooks[exchange] = orderbook

    def get_unified_orderbook(self, symbol, size):
        client_orderbooks = []
        for curr_orderbook in self._orderbooks:
            client_orderbooks.append(self._orderbooks[curr_orderbook].get_current_partial_book(symbol, size))
        best_orders = {'asks' : [], 'bids' : []}
        order_keys = [[heapq.nsmallest, 'asks'], [heapq.nlargest, 'bids']]
        for curr_orderbook in client_orderbooks:
            for curr_keyset in order_keys:
                best_orders[curr_keyset[1]] = curr_keyset[0](size, best_orders[curr_keyset[1]] + curr_orderbook[curr_keyset[1]], key=operator.itemgetter('price'))

        return best_orders