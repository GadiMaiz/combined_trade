from bitex.api.WSS.bitstamp import BitstampWSS
from orderbook_base import OrderbookBase
from threading import Thread
import json

class BitstampOrderbook(OrderbookBase):
    def __init__(self, asset_pairs, **kwargs):
        super().__init__(asset_pairs)
        self._bitstamp_wss_listener = BitstampWSS(**kwargs)
        self._listener_thread = None
        self.running = False

    def _start(self):
        self._bitstamp_wss_listener.start()
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._listener_thread = Thread(target=self.handle_q,
                                           daemon=True,
                                           name='Listen to Bitstamp Queue')
            self.running = True
            self._listener_thread.start()

    def _stop(self):
        self.running = False
        self._bitstamp_wss_listener.stop()
        while not self._bitstamp_wss_listener.data_q.empty():
            self._bitstamp_wss_listener.data_q.get()
        for curr_task_index in range(self._bitstamp_wss_listener.data_q.unfinished_tasks):
            self._bitstamp_wss_listener.data_q.task_done()
        self._bitstamp_wss_listener.data_q.join()
        if self._listener_thread is not None and self._listener_thread.is_alive:
            self._listener_thread.join()

    def get_current_partial_book(self, asset_pair, book_size):
        asset_pair_dict = {'BTC-USD': 'BTC', 'BCH-USD': 'BCH', 'BTC': 'BTC', 'BCH': 'BCH'}
        return self._bitstamp_wss_listener.get_current_partial_book(asset_pair_dict[asset_pair], book_size)

    def is_orderbook_thread_alive(self):
        return True

    def handle_q(self):
        trade_types = {1: "sell", 0: "buy"}
        asset_pair_dict = {'BTCUSD': 'BTC-USD', 'BCHUSD': 'BCH-USD'}
        while self.running:
            data = self._bitstamp_wss_listener.data_q.get()
            if data[0] == 'live_trades':
                pair = data[1]
                trade_dict = json.loads(data[2])
                self._last_trade[asset_pair_dict[pair]] = {"price": trade_dict["price"],
                                                           "type": trade_types[trade_dict["type"]],
                                                           "time": trade_dict["timestamp"]}
            self._bitstamp_wss_listener.data_q.task_done()