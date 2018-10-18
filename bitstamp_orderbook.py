from bitex.api.WSS.bitstamp import BitstampWSS
from orderbook_base import OrderbookBase
from threading import Thread
import json


class BitstampOrderbook(OrderbookBase):
    def __init__(self, asset_pairs, fees, **kwargs):
        super().__init__(asset_pairs, fees)
        self._orderbook_args = kwargs
        self._bitstamp_wss_listener = None
        self._listener_thread = None
        self.running = False

    def _start(self):
        self._bitstamp_wss_listener = BitstampWSS(**self._orderbook_args)
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

    def _get_orderbook_from_exchange(self, asset_pair, book_size):
        #asset_pair_dict = {'BTC-USD': 'BTC', 'BCH-USD': 'BCH', 'BTC': 'BTC', 'BCH': 'BCH'}
        if self._bitstamp_wss_listener:
            orders = self._bitstamp_wss_listener.get_current_partial_book(asset_pair, book_size)
        else:
            orders = {'asks': [], 'bids': []}
        return orders

    def is_orderbook_thread_alive(self):
        return True

    def handle_q(self):
        trade_types = {1: "sell", 0: "buy"}
        asset_pair_dict = {'BTCUSD': 'BTC-USD', 'BCHUSD': 'BCH-USD', 'LTCUSD': 'LTC-USD'}
        while self.running:
            data = self._bitstamp_wss_listener.data_q.get()
            if data[0] == 'live_trades':
                pair = data[1]
                trade_dict = json.loads(data[2])
                bitstamp_pair = pair
                if pair in asset_pair_dict:
                    bitstamp_pair = asset_pair_dict[pair]
                self._last_trade[bitstamp_pair] = {"price": trade_dict["price"],
                                                   "type": trade_types[trade_dict["type"]],
                                                   "time": trade_dict["timestamp"]}
                self._updated_listened_orders(trade_dict)
                self._track_trade_info(trade_dict, bitstamp_pair)
            self._bitstamp_wss_listener.data_q.task_done()

    def _updated_listened_orders(self, trade_info):
        order_id = None
        if trade_info['buy_order_id'] in self._orders_for_listening:
            order_id = trade_info['buy_order_id']
        elif trade_info['sell_order_id'] in self._orders_for_listening:
            order_id = trade_info['sell_order_id']
        if order_id:
            print("Order id found:", order_id)
            self._orders_for_listening[order_id].order_changed(trade_info['amount'], trade_info['price'],
                                                               trade_info['timestamp'])

    def _track_trade_info(self, trade_dict, asset_pair):
        # types: 0 - buy, 1 - sell
        if trade_dict['type'] == 1:
            self._rate_trackers[asset_pair]['sell'].add_trade(trade_dict['amount'], trade_dict['price'])
        else:
            self._rate_trackers[asset_pair]['buy'].add_trade(trade_dict['amount'], trade_dict['price'])
        #print("Bitstamp type={}, size={}, price={}".format(trade_dict['type'], trade_dict['amount'],
        #                                                   trade_dict['price']))
