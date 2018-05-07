from bitex.api.WSS.bitstamp import BitstampWSS
from orderbook_base import OrderbookBase

class BitstampOrderbook(OrderbookBase):
    def __init__(self, asset_pairs):
        super().__init__(asset_pairs)
        self._bitstamp_wss_listener = BitstampWSS()

    def _start(self):
        self._bitstamp_wss_listener.start()

    def _stop(self):
        self._bitstamp_wss_listener.stop()

    def get_current_partial_book(self, asset_pair, book_size):
        asset_pair_dict = {'BTC-USD': 'BTC', 'BCH-USD': 'BCH', 'BTC': 'BTC', 'BCH': 'BCH'}
        return self._bitstamp_wss_listener.get_current_partial_book(asset_pair_dict[asset_pair], book_size)