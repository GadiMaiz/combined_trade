import logging
from trade_db import TradeDB
from multiple_exchanges_client_wrapper import MultipleExchangesClientWrapper
from unified_orderbook import UnifiedOrderbook
import time


class ExchangeClientManager:
    DEFAULT_ACCOUNT = "smart_trade_default"

    def __init__(self, exchanges_params, db_file, watchdog, trades_update_url):
        self.log = logging.getLogger('smart-trader')
        self._reserved_balances = {'BTC': 0, 'BCH': 0, 'USD': 0}
        self._default_account = ExchangeClientManager.DEFAULT_ACCOUNT
        self._clients = dict()
        self._db_interface = TradeDB(db_file, trades_update_url)
        self._orderbooks = dict()
        self._watchdog = watchdog
        self._sent_orders_multiple_exchanges_identifier = 0
        self._multiple_clients = dict()
        self._multiple_clients_by_external_order_id = dict()
        self._last_multiple_client_timed_status = dict()
        self._last_status_by_currency_to = dict()
        self._last_status_by_external_order_id = dict()
        self._exchange_params = exchanges_params
        self._create_account_exchange_clients(self._default_account)
        for curr_exchange in exchanges_params:
            self._orderbooks[curr_exchange] = exchanges_params[curr_exchange]['args']['orderbook']

    def _create_account_exchange_clients(self, account):
        self._clients[account] = dict()
        for curr_exchange in self._exchange_params:
            self._clients[account][curr_exchange] = {}
            self._clients[account][curr_exchange]['client'] = self._exchange_params[curr_exchange]['creator'](
                db_interface=self._db_interface, clients_manager=self, account=account,
                **self._exchange_params[curr_exchange]['args'])

    @staticmethod
    def check_default_account(account):
        if account is None:
            account = ExchangeClientManager.DEFAULT_ACCOUNT
        return account

    def exchange_currency_balance(self, exchange, currency, account):
        result = None
        account = ExchangeClientManager.check_default_account(account)
        if account in self._clients:
            result = self._clients[account][exchange]['client'].account_balance(currency)
            result['exchange'] = exchange
        return result

    def exchange_assets(self, exchange, account):
        result = {'exchange': exchange,
                  'assetPairs': []}
        account = ExchangeClientManager.check_default_account(account)
        if account in self._clients and exchange in self._clients[account]:
            result['assetPairs'] = self._orderbooks[account][exchange]['orderbook'].get_asset_pairs()
        return result

    def exchange_balance(self, exchange, account, extended_info=True):
        result = dict()
        account = ExchangeClientManager.check_default_account(account)
        if account in self._clients and exchange in self._clients[account]:
            result = self._clients[account][exchange]['client'].account_balance(extended_info=extended_info)
        if extended_info:
            result['exchange'] = exchange
        return result

    def get_all_account_balances(self, force_exchange, account):
        result = dict()
        account = ExchangeClientManager.check_default_account(account)
        unified_balance = {'total_usd_value': 0,
                           'reserved_crypto_type': '',
                           'reserved_crypto:': 0,
                           'server_usd_reserved': 0}
        if account in self._clients:
            for exchange in self._clients[account]:
                if force_exchange:
                    self._clients[account][exchange]['client'].set_balance_changed()
                result[exchange] = self.exchange_balance(exchange, account)
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

    def set_exchange_credentials(self, exchange, credentials, account):
        account = ExchangeClientManager.check_default_account(account)
        result = {'set_credentials_status': 'False'}
        if exchange in self._clients[ExchangeClientManager.DEFAULT_ACCOUNT]:
            if account not in self._clients:
                self._create_account_exchange_clients(account)
            result = self._clients[account][exchange]['client'].set_credentials(credentials)
        return result

    def get_signed_in_credentials(self, exchange, account):
        account = ExchangeClientManager.check_default_account(account)
        result = {'signed_in_user': "", 'is_user_signed_in': str(False)}
        if account in self._clients and exchange in self._clients[account]:
            result = self._clients[account][exchange]['client'].get_signed_in_credentials()
        return result

    def logout_from_exchange(self, exchange, account):
        account = ExchangeClientManager.check_default_account(account)
        result = False
        if account in self._clients and exchange in self._clients[account]:
            result = self._clients[account][exchange]['client'].logout()
        return result

    def get_exchange_transactions(self, exchange, limit, account):
        account = ExchangeClientManager.check_default_account(account)
        result = []
        if account in self._clients and exchange in self._clients[account]:
            result = self._clients[exchange]['client'].transactions(limit)
        return result

    def send_order(self, exchanges, action_type, currency_to_size, currency_to_type, currency_from_size,
                   currency_from_type, duration_sec, max_order_size, account, external_order_id='', user_quote_price=0,
                   user_id='', max_exchange_sizes=dict()):
        account = ExchangeClientManager.check_default_account(account)
        self.log.info("send_order: exchanges: <%s>, action_type: <%s>, currency_to_size: <%f>, currency_to_type: <%s> "
                      "currency_from_size: <%f> for account <%s>", exchanges, action_type, currency_to_size,
                      currency_to_type, currency_from_size, account)
        if account not in self._clients:
            self.log.error("Account <%s> doesn't existm can't send order", account)
            result = {'order_status': "Account {} doesn't exist".format(account)}
        else:
            active_exchanges = self._watchdog.get_active_exchanges()
            order_exchanges = []
            for exchange in exchanges:
                if exchange in active_exchanges:
                    order_exchanges.append(exchange)
            result = {'order_status': False, 'execution_size': 0, 'execution_message': 'Invalid exchanges'}
            if len(order_exchanges) == 0:
                self.log.error("Can't execute order for exchanges <%s> because none of them is in the active_exchanges"
                               " list: <%s>", exchanges, active_exchanges)
            elif len(order_exchanges) == 1 and order_exchanges[0] in self._clients[account]:
                #if duration_sec > 0:
                self.log.info("Sending order to <%s>", order_exchanges[0])
                result = self._clients[account][order_exchanges[0]]['client'].send_order(
                    action_type, currency_to_size, currency_to_type, currency_from_size, currency_from_type,
                    duration_sec, max_order_size, True, external_order_id, user_quote_price, user_id)
            else:
                account_clients = self._clients[account]
                valid_exchanges = True
                order_clients = dict()
                curr_order_orderbooks = dict()
                for exchange in order_exchanges:
                    if exchange in account_clients:
                        order_clients[exchange] = account_clients[exchange]['client']
                        curr_order_orderbooks[exchange] = self._orderbooks[exchange]['orderbook']
                    else:
                        result = {'order_status': "Exchange {} doesn't exist".format(exchange)}
                        valid_exchanges = False
                        break

                if valid_exchanges:
                    orderbook_for_order = UnifiedOrderbook(curr_order_orderbooks)
                    multiple_client = MultipleExchangesClientWrapper(order_clients,
                                                                     {'orderbook': orderbook_for_order},
                                                                     self._db_interface,
                                                                     self._watchdog,
                                                                     self._sent_orders_multiple_exchanges_identifier,
                                                                     self, account)
                    self._multiple_clients[self._sent_orders_multiple_exchanges_identifier] = \
                        {'account': account, 'client': multiple_client}
                    self._sent_orders_multiple_exchanges_identifier = \
                        self._sent_orders_multiple_exchanges_identifier + 1
                    if external_order_id:
                        self._multiple_clients_by_external_order_id[external_order_id] = multiple_client
                    result = multiple_client.send_order(
                        action_type, currency_to_size, currency_to_type,  currency_from_size, currency_from_type,
                        duration_sec, max_order_size, True, external_order_id, user_quote_price, user_id, -1,
                        max_exchange_sizes)
        return result

    def is_timed_order_running(self, account):
        account = ExchangeClientManager.check_default_account(account)
        result = False
        timed_order_client = self._get_timed_order_client(account, None, None)
        if timed_order_client:
            result = True
        return result

    def get_timed_order_status(self, account, currency_to, external_order_id):
        account = ExchangeClientManager.check_default_account(account)
        result = {'timed_order_running': False,
                  'action_type': "",
                  'timed_order_required_size': 0,
                  'timed_order_done_size': 0,
                  'timed_order_sent_time': 0,
                  'timed_order_execution_start_time': 0,
                  'timed_order_elapsed_time': 0,
                  'timed_order_duration_sec': 0,
                  'timed_order_price_fiat': 0}

        timed_order_client = self._get_timed_order_client(account, currency_to, external_order_id)
        if timed_order_client:
            result = timed_order_client.get_timed_order_status(currency_to)
        elif account in self._last_multiple_client_timed_status and \
                self._last_multiple_client_timed_status[account] is not None:
            result = self._last_multiple_client_timed_status[account]
        result['server_time'] = time.time()
        return result

    def cancel_timed_order(self, account, currency_to, external_order_id):
        account = ExchangeClientManager.check_default_account(account)
        result = False
        timed_order_client = self._get_timed_order_client(account, currency_to, external_order_id)
        if timed_order_client:
            result = timed_order_client.cancel_timed_order(currency_to, external_order_id)
        return result

    def get_sent_orders(self, order_type, limit, query_filter=None):
        return self._db_interface.get_sent_orders(order_type, limit, query_filter)

    def unregister_client(self, identifier, external_order_id):
        self._multiple_clients.pop(identifier, None)
        if external_order_id:
            self._multiple_clients_by_external_order_id.pop(external_order_id, None)

    def _get_timed_order_client(self, account, currency_to, external_order_id):
        account = ExchangeClientManager.check_default_account(account)
        timed_order_client = None
        if external_order_id in self._multiple_clients_by_external_order_id:
            timed_order_client = self._multiple_clients_by_external_order_id[external_order_id]
        elif account in self._clients:
            for multiple_client_identifier in self._multiple_clients:
                if self._multiple_clients[multiple_client_identifier]['account'] == account and\
                        self._multiple_clients[multiple_client_identifier]['client'].is_timed_order_running(
                            currency_to):
                    timed_order_client = self._multiple_clients[multiple_client_identifier]['client']
                    break

            if not timed_order_client:
                for curr_exchange in self._clients[account]:
                    if self._clients[account][curr_exchange]['client'].is_timed_order_running(currency_to):
                        timed_order_client = self._clients[account][curr_exchange]['client']
                        break

        return timed_order_client

    def set_last_status(self, last_status, account, external_order_id, currency_to):
        account = ExchangeClientManager.check_default_account(account)
        self._last_multiple_client_timed_status[account] = last_status
        if currency_to:
            if account not in self._last_status_by_currency_to:
                self._last_status_by_currency_to[account] = dict()
            self._last_status_by_currency_to[account][currency_to] = last_status
        if last_status:
            self._last_status_by_external_order_id[external_order_id] = last_status
