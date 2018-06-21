from bitex.api.WSS import BitfinexWSS
from threading import Thread
from orderbook_base import OrderbookBase
import logging

log = logging.getLogger(__name__)

class BitfinexOrderbook(OrderbookBase):
    symbols_dict = {'BTCUSD': 'BTC', 'BCHUSD': 'BCH', 'BTC-USD': 'BTC', 'BCH-USD': 'BCH'}
    pairs_dict = {'BTCUSD': 'BTC-USD', 'BCHUSD': 'BCH-USD'}

    def __init__(self, asset_pairs, fees):
        self._external_asset_pairs = []
        self._inverse_pairs = {}
        for curr_pair in BitfinexOrderbook.pairs_dict:
            if curr_pair in asset_pairs:
                self._external_asset_pairs.append(BitfinexOrderbook.pairs_dict[curr_pair])
                self._inverse_pairs[BitfinexOrderbook.pairs_dict[curr_pair]] = curr_pair
        super().__init__(asset_pairs, fees)
        self._running = False
        self._orderbook_thread = None
        self._orderbook = {'BTC': None, 'BCH': None}
    def _start(self):

        self._bitfinex_client = BitfinexWSS()
        self._bitfinex_client.start()
        if self._orderbook_thread is None or not self._orderbook_thread.is_alive():
            self._orderbook_thread = Thread(target=self._manage_orderbook,
                                             daemon=True,
                                             name='Manage Orderbook Thread')
            self._running = True
            self._orderbook_thread.start()

    def _stop(self):
        """
        Stops Threads. Overwrite this in your child class as necessary.
        :return:
        """
        if self._bitfinex_client is not None:
            self._bitfinex_client.stop()
        self._running = False

    def _manage_orderbook(self):
        log.info("running manage orderbook thread")
        orderbook_init = { 'BTCUSD': False, 'BCHUSD': False}
        while self._running:
            try:
                curr_info = self._bitfinex_client.data_q.get()
                if curr_info[0] == 'order_book' and curr_info[1] in orderbook_init.keys():
                    if not orderbook_init[curr_info[1]]:
                        #print ("init orderbook", orderbook_init)
                        self._init_orderbook(curr_info)
                        orderbook_init[curr_info[1]] = True
                    else:
                        #print("modify orderbook")
                        self._modify_orderbook(curr_info)
                elif curr_info[0] == 'trades' and curr_info[1] in orderbook_init.keys():
                    self._set_price(curr_info)

            except Exception as e:
                log.error(str(e))

    def _init_orderbook(self, first_orderbook):
        #print ("_init_orderbook")
        asks = []
        bids = []
        all_orders = first_orderbook[2][0][0]
        for curr_order in all_orders:
            book_order = {'price' : float(curr_order[0]),
                          'orders_num': int(curr_order[1]),
                          'size' : abs(float(curr_order[2])),
                          'source': 'Bitfinex'}
            if float(curr_order[2]) < 0:
                asks.append(book_order)
            else:
                bids.append(book_order)

        self._orderbook[self.symbols_dict[first_orderbook[1]]] = {'asks': asks,
                           'bids': bids}
        #print (self._orderbook)

    def _modify_orderbook(self, orderbook_change):
        crypto_type = self.symbols_dict[orderbook_change[1]]
        change_price = float(orderbook_change[2][0][0][0])
        change_orders_count = int(orderbook_change[2][0][0][1])
        change_amount = float(orderbook_change[2][0][0][2])

        if change_orders_count > 0:
            index = 0
            element_type = None
            if change_amount > 0:
                element_type = 'bids'
                while (index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] > change_price):
                    #print (index, self._orderbook[element_type])
                    index += 1
            else:
                element_type = 'asks'
                while (index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] < change_price):
                    # print (index, self._orderbook[element_type])
                    index += 1

            # Change an existing element
            if (index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] == change_price):
                self._orderbook[crypto_type][element_type][index]['orders_num'] = change_orders_count
                self._orderbook[crypto_type][element_type][index]['size'] = abs(change_amount)

            # Add a new element
            else:
                #print(orderbook_change)
                #print (element_type, "add new element")
                new_element = {'price' : change_price,
                          'orders_num': change_orders_count,
                          'size' : abs(change_amount),
                          'source': 'Bitfinex'}
                self._orderbook[crypto_type][element_type].insert(index, new_element)
                #print("new element", element, "orderbook", self._orderbook[element_type])
        elif change_orders_count == 0:
            element_type = None
            index = 0
            if change_amount == 1:
                element_type = 'bids'
                while (index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] > change_price):
                    index += 1
            else:
                element_type = 'asks'
                while (index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] < change_price):
                    index += 1

            # Delete an existing bid
            if index < len(self._orderbook[crypto_type][element_type]) and self._orderbook[crypto_type][element_type][index]['price'] == change_price:
                del self._orderbook[crypto_type][element_type][index]

    def _get_orderbook_from_exchange(self, asset_pair, size):
        crypto_type = self.symbols_dict[asset_pair]
        result = { 'asks': [],
                   'bids': []}
        if self._orderbook[crypto_type] != None:
            result = { 'asks': self._orderbook[crypto_type]['asks'][0:size],
                       'bids': self._orderbook[crypto_type]['bids'][0:size]}

        return result

    def is_orderbook_thread_alive(self):
        is_alive = False
        if self._running and self._orderbook_thread is not None and self._orderbook_thread.is_alive() and \
            self._bitfinex_client.receiver_thread is not None and self._bitfinex_client.receiver_thread.is_alive() and \
            self._bitfinex_client.processing_thread is not None and self._bitfinex_client.processing_thread.is_alive():
                is_alive = True

        return is_alive

    def _set_price(self, message):
        if not isinstance(message[2][0][0], list):
            trade_type = 'buy'
            if float(message[2][0][1][2]) < 0:
                trade_type = 'sell'
            self._last_trade[self.pairs_dict[message[1]]] = {'type': trade_type,
                                                             "price": abs(float(message[2][0][1][3])),
                                                             "time": message[2][0][1][1] / 1000}
            self._track_trade_info(message, self._inverse_pairs[self.pairs_dict[message[1]]])

    def get_asset_pairs(self):
        return self._external_asset_pairs

    def _track_trade_info(self, trade_dict, asset_pair):
        #print(trade_dict)
        if trade_dict[2][0][0] == 'te':
            #print("Bitfinex {} size={}, price={}".format(asset_pair, float(trade_dict[2][0][1][2]),
            #                                             float(trade_dict[2][0][1][3])))
            try:
                if float(trade_dict[2][0][1][2]) > 0:
                    self._rate_trackers[asset_pair]['buy'].add_trade(float(trade_dict[2][0][1][2]),
                                                                     float(trade_dict[2][0][1][3]))
                else:
                    #print("Sell:", trade_dict)
                    self._rate_trackers[asset_pair]['sell'].add_trade(-1 * float(trade_dict[2][0][1][2]),
                                                                      float(trade_dict[2][0][1][3]))
            except Exception as e:
                print("Bitfinex exception", e)
            #print(self.get_tracked_info(asset_pair))

    def get_tracked_info(self, asset_pair):
        asset_pair = self._inverse_pairs[asset_pair]
        return super().get_tracked_info(asset_pair)
