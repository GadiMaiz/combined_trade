import logging
from trade_db import TradeDB
from multiple_exchanges_client_wrapper import MultipleExchangesClientWrapper
from unified_orderbook import UnifiedOrderbook

class ExchangeClientManager():
    def __init__(self, exchanges_params, db_file, watchdog):
        self.log = logging.getLogger(__name__)
        self._reserved_balances = {'BTC': 0, 'BCH': 0, 'USD': 0}
        self._clients = dict()
        self._db_interface = TradeDB(db_file)
        self._orderbooks = dict()
        self._watchdog = watchdog
        self._sent_orders_multiple_exchanges_identifier = 0
        self._multiple_clients = dict()
        self._last_multiple_client_timed_status = None
        for curr_exchange in exchanges_params:
            self._clients[curr_exchange] = {}
            self._clients[curr_exchange]['client'] = exchanges_params[curr_exchange]['creator']\
                (db_interface=self._db_interface, clients_manager=self, **exchanges_params[curr_exchange]['args'])
            self._orderbooks[curr_exchange] = exchanges_params[curr_exchange]['args']['orderbook']

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

    def send_order(self, exchanges, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec,
                   max_order_size):
        result = dict()
        if len(exchanges) == 1 and exchanges[0] in self._clients:
            #if duration_sec > 0:
            print("Sending order to", exchanges[0])
            result = self._clients[exchanges[0]]['client'].send_order(action_type, size_coin, crypto_type,
                                                                      price_fiat, fiat_type, duration_sec,
                                                                      max_order_size)
        else:
            valid_exchanges = True
            order_clients = dict()
            curr_order_orderbooks = dict()
            for exchange in exchanges:
                if exchange in self._clients:
                    order_clients[exchange] = self._clients[exchange]['client']
                    curr_order_orderbooks[exchange] = self._orderbooks[exchange]['orderbook']
                else:
                    result = {'order_status': "Exchange {} doesn't exist".format(exchange)}
                    valid_exchanges = False
                    break

            if valid_exchanges:
                orderbook_for_order = UnifiedOrderbook(curr_order_orderbooks)
                multiple_client = MultipleExchangesClientWrapper(order_clients,
                                                                 orderbook_for_order, self._db_interface,
                                                                 self._watchdog,
                                                                 self._sent_orders_multiple_exchanges_identifier,
                                                                 self)
                self._multiple_clients[self._sent_orders_multiple_exchanges_identifier] = multiple_client
                self._sent_orders_multiple_exchanges_identifier = self._sent_orders_multiple_exchanges_identifier + 1
                result = multiple_client.send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                                    duration_sec,  max_order_size)
        return result

    def is_timed_order_running(self):
        result = False
        timed_order_client = self._get_timed_order_client()
        if timed_order_client:
            result = True
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

        timed_order_client = self._get_timed_order_client()
        if timed_order_client:
            result = timed_order_client.get_timed_order_status()
        elif self._last_multiple_client_timed_status is not None:
            result = self._last_multiple_client_timed_status
        return result

    def cancel_timed_order(self):
        result = False
        timed_order_client = self._get_timed_order_client()
        if timed_order_client:
            result = timed_order_client.cancel_timed_order()
        return result

    def get_sent_orders(self, limit):
        return self._db_interface.get_sent_orders(limit)

    def unregister_client(self, identifier):
        self._multiple_clients.pop(identifier, None)

    def _get_timed_order_client(self):
        timed_order_client = None
        for curr_exchange in self._clients:
            if self._clients[curr_exchange]['client'].is_timed_order_running():
                timed_order_client = self._clients[curr_exchange]['client']
                break

        if not timed_order_client:
            for multiple_client_identifier in self._multiple_clients:
                if self._multiple_clients[multiple_client_identifier].is_timed_order_running():
                    timed_order_client = self._multiple_clients[multiple_client_identifier]
                    break
        return timed_order_client

    def set_last_status(self, last_status):
        self._last_multiple_client_timed_status = last_status