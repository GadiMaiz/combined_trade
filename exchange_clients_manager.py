import logging
from trade_db import TradeDB

class ExchangeClientManager():
    def __init__(self, exchanges_params, db_file):
        self.log = logging.getLogger(__name__)
        self._reserved_balances = {'BTC': 0, 'BCH': 0, 'USD': 0}
        self._clients = {}
        self._db_interface = TradeDB(db_file)
        self._timed_order_exchange = None
        for curr_exchange in exchanges_params:
            self._clients[curr_exchange] = {}
            self._clients[curr_exchange]['client'] = exchanges_params[curr_exchange]['creator']\
                (db_interface=self._db_interface, **exchanges_params[curr_exchange]['args'])

    def exchange_currency_balance(self, exchange, currency):
        result = self._clients[exchange]['client'].account_balance(currency)
        result['exchange'] = exchange
        return result

    def exchange_balance(self, exchange):
        result = self._clients[exchange]['client'].account_balance()
        result['exchange'] = exchange
        return result

    def get_all_account_balances(self):
        result = {'exchanges': []}
        for exchange in self._clients:
            result['exchanges'][exchange] = self.exchange_balance(exchange)
        result['reserved_balances'] = self._reserved_balances
        return result

    def set_exchange_credentials(self, exchange, credentials):
        return self._clients[exchange]['client'].set_credentials(credentials)

    def get_signed_in_credentials(self, exchange):
        return self._clients[exchange]['client'].get_signed_in_credentials()

    def logout_from_exchange(self, exchange):
        return self._clients[exchange]['client'].logout()

    def get_exchange_transactions(self, exchange, limit):
        return self._clients[exchange]['client'].transactions(limit)

    def send_order(self, exchange, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec,
                   max_order_size):
        if exchange in self._clients:
            if duration_sec > 0:
                self._timed_order_exchange = exchange
            return self._clients[exchange]['client'].send_order(action_type, size_coin, crypto_type, price_fiat,
                                                                fiat_type, duration_sec, max_order_size)
        else:
            return {'order_status': "Exchange {} doesn't exist".format(exchange)}

    def is_timed_order_running(self):
        result = False
        for curr_exchange in self._clients:
            if self._clients[curr_exchange]['client'].is_timed_order_running():
                result = True
                break
        return result

    def get_timed_order_status(self):
        result = {'timed_order_running': False,
                  'action_type': "",
                  'timed_order_required_size': 0,
                  'timed_order_done_size': 0,
                  'timed_order_sent_time': 0,
                  'timed_order_execution_start_time': 0,
                  'timed_order_elapsed_time': 0,
                  'timed_order_duration_sec': 0,
                  'timed_order_price_fiat': 0}
        if self._timed_order_exchange is not None:
            result = self._clients[self._timed_order_exchange]['client'].get_timed_order_status()

        return result

    def cancel_timed_order(self):
        result = False
        if self._timed_order_exchange is not None:
            result = self._clients[self._timed_order_exchange]['client'].cancel_timed_order()

        return result

    def get_sent_orders(self, limit):
        return self._db_interface.get_sent_orders(limit)