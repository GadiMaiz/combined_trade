import logging
import datetime

class OrderTracker:
    def __init__(self, order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order):
        self._log = self.log = logging.getLogger('smart-trader')
        self._order = order
        self._orderbook = orderbook
        self._client_wrapper = client_wrapper
        self._order_info = order_info
        self._listening = True
        self._currency_from = currency_from
        self._currency_to = currency_to
        self._timed_order = tracked_order
        self._initial_size = client_wrapper.get_timed_order_status(currency_to)['timed_order_done_size']
        orderbook['orderbook'].listen_for_order(order["id"], self)

    def order_changed(self, executed_size, price, timestamp):
        self._log.debug("Order changed: <%s>, executed_size <%f>, price <%f>, timestamp <%s>", type(self),
                        executed_size, price, str(timestamp))
        self._order['executed_size'] += executed_size
        if self._order['executed_size'] >= self._order['required_size']:
            self.unregister_order()
        self._client_wrapper.add_order_executed_size(executed_size, price, self._order_info, timestamp,
                                                     self._timed_order)

    def update_order_from_transactions(self):
        pass

    def unregister_order(self):
        if self._listening:
            self._listening = False
            self._orderbook['orderbook'].stop_listening_for_order(self._order['id'])

    def update_order_from_exchange(self):
        pass


class BitfinexOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order):
        super().__init__(order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order)

    def update_order_from_exchange(self):
        order_status = self._client_wrapper.order_status(self._order['id'])
        executed_size = 0
        try:
            if order_status:
                executed_size = float(order_status['executed_size'])
                self._order['executed_size'] = executed_size
        except Exception as e:
            self._log.debug("Order status: <%s>", order_status)
        self._client_wrapper.set_order_executed_size(executed_size + self._initial_size, self._timed_order)


class KrakenOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order):
        super().__init__(order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order)

    def update_order_from_exchange(self):
        order_status = self._client_wrapper.order_status(self._order['id'])
        executed_size = 0
        self._log.debug("Kraken order status: <%s>", order_status)
        if order_status and self._order['id'] in order_status:
            executed_size = float(order_status[self._order['id']]['vol_exec'])
            self._order['executed_size'] = executed_size
            if 'closetm' in order_status[self._order['id']]:
                tracked_order_timestamp = datetime.datetime.utcfromtimestamp(order_status[self._order['id']]['closetm'])
                (dt, micro) = tracked_order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
                self._order_info['order_time'] = "%s.%02d" % (dt, int(micro) / 1000)
        self._client_wrapper.set_order_executed_size(executed_size + self._initial_size, self._timed_order)


class BitstampOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order):
        super().__init__(order, orderbook, client_wrapper, order_info, currency_from, currency_to, tracked_order)

    def update_order_from_transactions(self):
        order_transactions = self._client_wrapper.get_order_status_from_transactions(self._order['id'],
                                                                                     self._currency_from,
                                                                                     self._currency_to)
        self._client_wrapper.set_order_executed_size(order_transactions['executed_size'] + self._initial_size,
                                                     self._timed_order)
        self._order['executed_size'] = order_transactions['executed_size']
        self._order['executed_price'] = order_transactions['transaction_price']
        self._order['updated_from_transactions'] = True
        self._log.debug("Updated Bitstamp order from transactions: <%s>", order_transactions)
