import logging
from trade_db import TradeDB
from multiple_exchanges_client_wrapper import MultipleExchangesClientWrapper
from unified_orderbook import UnifiedOrderbook
import time


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

    def exchange_balance(self, exchange, extended_info=True):
        result = dict()
        if exchange in self._clients:
            result = self._clients[exchange]['client'].account_balance(extended_info=extended_info)
        if extended_info:
            result['exchange'] = exchange
        return result

    def get_all_account_balances(self, force_exchange):
        result = dict()
        unified_balance = {'total_usd_value': 0,
                           'reserved_crypto_type': '',
                           'reserved_crypto:': 0,
                           'server_usd_reserved': 0}
        for exchange in self._clients:
            if force_exchange:
                self._clients[exchange]['client'].set_balance_changed()
            result[exchange] = self.exchange_balance(exchange)
            if 'total_usd_value' in result[exchange]:
                unified_balance['total_usd_value'] += result[exchange]['total_usd_value']
            if 'balances' in result[exchange]:
                if 'balances' not in unified_balance:
                    unified_balance['balances'] = dict()
                for currency in result[exchange]['balances']:
                    if currency not in unified_balance['balances']:
                        unified_balance['balances'][currency] = dict()
                        unified_balance['balances'][currency]['available'] = \
                            result[exchange]['balances'][currency]['available']
                        unified_balance['balances'][currency]['amount'] = \
                            result[exchange]['balances'][currency]['amount']
                    else:
                        unified_balance['balances'][currency]['available'] += \
                            result[exchange]['balances'][currency]['available']
                        unified_balance['balances'][currency]['amount'] += \
                            result[exchange]['balances'][currency]['amount']
        #result['reserved_balances'] = self._reserved_balances
        result['Unified'] = unified_balance
        return result

    def set_exchange_credentials(self, exchange, credentials):
        if exchange in self._clients:
            return self._clients[exchange]['client'].set_credentials(credentials)
        else:
            return {'set_credentials_status': 'False'}

    def get_signed_in_credentials(self, exchange):
        if exchange in self._clients:
            return self._clients[exchange]['client'].get_signed_in_credentials()
        else:
            return {'signed_in_user': "", 'is_user_signed_in': str(False)}

    def logout_from_exchange(self, exchange):
        if exchange in self._clients:
            return self._clients[exchange]['client'].logout()

    def get_exchange_transactions(self, exchange, limit):
        return self._clients[exchange]['client'].transactions(limit)

    def send_order(self, exchanges, action_type, currency_to_size, currency_to_type, currency_from_size,
                   currency_from_type, duration_sec, max_order_size,  external_order_id='', user_quote_price=0,
                   user_id=''):
        self.log.info("send_order: exchanges: <%s>, action_type: <%s>, currency_to_size: <%f>, currency_to_type: <%s> "
                      "currency_from_size: <%f>", exchanges, action_type, currency_to_size, currency_to_type,
                      currency_from_size)
        active_exchanges = self._watchdog.get_active_exchanges()
        order_exchanges = []
        for exchange in exchanges:
            if exchange in active_exchanges:
                order_exchanges.append(exchange)
        result = {'order_status': False, 'execution_size': 0, 'execution_message': 'Invalid exchanges'}
        if len(order_exchanges) == 1 and order_exchanges[0] in self._clients:
            #if duration_sec > 0:
            self.log.info("Sending order to <%s>", order_exchanges[0])
            result = self._clients[order_exchanges[0]]['client'].send_order(action_type, currency_to_size,
                                                                            currency_to_type, currency_from_size,
                                                                            currency_from_type, duration_sec,
                                                                            max_order_size, True, external_order_id,
                                                                            user_quote_price, user_id)
        else:
            valid_exchanges = True
            order_clients = dict()
            curr_order_orderbooks = dict()
            for exchange in order_exchanges:
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
                                                                 {'orderbook': orderbook_for_order}, self._db_interface,
                                                                 self._watchdog,
                                                                 self._sent_orders_multiple_exchanges_identifier,
                                                                 self)
                self._multiple_clients[self._sent_orders_multiple_exchanges_identifier] = multiple_client
                self._sent_orders_multiple_exchanges_identifier = self._sent_orders_multiple_exchanges_identifier + 1
                result = multiple_client.send_order(action_type, currency_to_size, currency_to_type, currency_from_size,
                                                    currency_from_type, duration_sec,  max_order_size, True,
                                                    external_order_id, user_quote_price, user_id)
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
        #print(timed_order_client)
        if timed_order_client:
            result = timed_order_client.get_timed_order_status()
            #print("Getting last single status:", result)
        elif self._last_multiple_client_timed_status is not None:
            result = self._last_multiple_client_timed_status
            #print("Getting last multiple status:", result)
        result['server_time'] = time.time()
        return result

    def cancel_timed_order(self):
        result = False
        timed_order_client = self._get_timed_order_client()
        if timed_order_client:
            result = timed_order_client.cancel_timed_order()
        return result

    def get_sent_orders(self, type, limit, filter=None):
        return self._db_interface.get_sent_orders(type, limit, filter)

    def unregister_client(self, identifier):
        self._multiple_clients.pop(identifier, None)

    def _get_timed_order_client(self):
        timed_order_client = None
        for multiple_client_identifier in self._multiple_clients:
            if self._multiple_clients[multiple_client_identifier].is_timed_order_running():
                timed_order_client = self._multiple_clients[multiple_client_identifier]
                break

        if not timed_order_client:
            for curr_exchange in self._clients:
                if self._clients[curr_exchange]['client'].is_timed_order_running():
                    timed_order_client = self._clients[curr_exchange]['client']
                    break

        return timed_order_client

    def set_last_status(self, last_status):
        self._last_multiple_client_timed_status = last_status
