class OrderTracker:
    def __init__(self, order, orderbook, client_wrapper, order_info, crypto_type):
        self._order = order
        self._orderbook = orderbook
        self._client_wrapper = client_wrapper
        self._order_info = order_info
        self._listening = True
        self._crypto_type = crypto_type
        self._initial_size = client_wrapper.get_timed_order_status()['timed_order_done_size']
        orderbook['orderbook'].listen_for_order(order["id"], self)

    def order_changed(self, executed_size, price, timestamp):
        print("Order changed:", type(self), executed_size, price, timestamp)
        self._order['executed_size'] += executed_size
        if self._order['executed_size'] >= self._order['required_size']:
            self.unregister_order()
        self._client_wrapper.add_order_executed_size(executed_size, price, self._order_info, timestamp)
        #print(self._client_wrapper.get_timed_order_status())

    def update_order_from_transactions(self):
        pass

    def unregister_order(self):
        if self._listening:
            self._listening = False
            self._orderbook['orderbook'].stop_listening_for_order(self._order['id'])

    def update_order_from_exchange(self):
        pass


class BitfinexOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, crypto_type):
        super().__init__(order, orderbook, client_wrapper, order_info, crypto_type)

    def update_order_from_exchange(self):
        order_status = self._client_wrapper.order_status(self._order['id'])
        executed_size = 0
        try:
            if order_status:
                executed_size = float(order_status['executed_size'])
        except Exception as e:
            print(order_status)
        self._client_wrapper.set_order_executed_size(executed_size + self._initial_size)


class KrakenOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, crypto_type):
        super().__init__(order, orderbook, client_wrapper, order_info, crypto_type)

    def update_order_from_exchange(self):
        order_status = self._client_wrapper.order_status(self._order['id'])
        executed_size = 0
        if order_status:
            print("order status:", order_status)
            executed_size = float(order_status[self._order['id']]['vol_exec'])
        self._client_wrapper.set_order_executed_size(executed_size + self._initial_size)


class BitstampOrderTracker(OrderTracker):
    def __init__(self, order, orderbook, client_wrapper, order_info, crypto_type):
        super().__init__(order, orderbook, client_wrapper, order_info, crypto_type)

    def update_order_from_transactions(self):
        order_transactions = self._client_wrapper.get_order_status_from_transactions(self._order['id'],
                                                                                     self._crypto_type)
        self._client_wrapper.set_order_executed_size(order_transactions['executed_size'] + self._initial_size)
        self._order['executed_size'] = order_transactions['executed_size']
        print("Updated Bitstamp order from transactions:", order_transactions)
