import logging


class ExchangeClientManager():
    def __init__(self, exchanges_params):
        self.log = logging.getLogger(__name__)
        self._reserved_balances = {'BTC': 0, 'BCH': 0, 'USD': 0}
        self._clients = {}
        for curr_exchange in exchanges_params:
            self._clients[curr_exchange] = {}
            self._clients[curr_exchange]['client'] = exchanges_params[curr_exchange]['creator']\
                (**exchanges_params[curr_exchange]['args'])

    def exchange_currency_balance(self, exchange, currency):
        result = self._clients[exchange].account_balance(currency)
        result['exchange'] = exchange
        return result

    def exchange_balance(self, exchange):
        result = self._clients[exchange].account_all_balances()
        result['exchange'] = exchange
        return result

    def get_all_account_balances(self):
        result = {'exchanges': []}
        for exchange in self._clients:
            result['exchanges'][exchange] = self.exchange_balance(exchange)
        result['reserved_balances'] = self._reserved_balances
        return result

    def set_exchange_credentials(self, exchange, **kwargs):
        return self._clients[exchange].set_credentials(**kwargs)

    def logout_from_exchange(self, exchange):
        return self._clients[exchange].logout()

    def get_exchange_transactions(self, exchange, limit):
        return self._clients[exchange].transactions(limit)

    def send_order(self, exchange, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec,
                   max_order_size):
        return self._clients[exchange].send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                                  duration_sec, max_order_size)