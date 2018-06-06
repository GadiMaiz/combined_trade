class TimedOrderExecuter():
    def __init__(self, client_wrapper, orderbook, asset_pair):
        self._client_wrapper = client_wrapper
        self._asset_pair = asset_pair
        self._orderbook = orderbook

    def get_current_spread_and_price(self):
        return self._orderbook['orderbook'].get_current_spread_and_price(self._asset_pair)

    def get_average_spread(self):
        return self._orderbook['orderbook'].get_average_spread(self._asset_pair)

    def minimum_order_size(self):
        return self._client_wrapper.minimum_order_size(self._asset_pair)

    def get_client_for_order(self):
        return self._client_wrapper
