import pykraken
import client_wrapper_base
import logging
import pykraken.kprivate
from kraken_orderbook import KrakenOrderbook
from order_tracker import KrakenOrderTracker


class KrakenClientWrapper(client_wrapper_base.ClientWrapperBase):
    def __init__(self, credentials, orderbook, db_interface, clients_manager):
        super().__init__(orderbook, db_interface, clients_manager)
        self.log = logging.getLogger(__name__)
        self._kraken_client = None
        self._signed_in_user = ""
        self.set_credentials(credentials)

    def set_credentials(self, client_credentials):
        username = ''
        key = ''
        secret = ''
        self.cancel_timed_order()
        try:
            if client_credentials is not None and 'username' in client_credentials and \
                    'key' in client_credentials and 'secret' in client_credentials:
                username = client_credentials['username']
                key = client_credentials['key']
                secret = client_credentials['secret']

            if len(username) != 0 and len(key) != 0 and len(secret) != 0:
                self._kraken_client = pykraken.Client(key=key, private_key=secret)
                self._signed_in_user = username
                self._balance_changed = True
                self._is_client_init = True
            else:
                self._signed_in_user = ''
                self._kraken_client = None
        except Exception as e:
            self.log.error("Sign in exception: {}".format(e))
            self._kraken_client = None
            self._signed_in_user = ''
            self._balance_changed = False
            self._is_client_init = False

        return self._kraken_client is not None

    def get_signed_in_credentials(self):
        signed_in_dict = {True: "True", False: "False"}
        return {'signed_in_user': self._signed_in_user, 'is_user_signed_in': signed_in_dict[self._signed_in_user != ""]}

    def logout(self):
        super().logout()
        return self._kraken_client is None

    def _get_balance_from_exchange(self):
        kraken_symbols_dict = {'ZUSD': 'USD', 'XXBT': 'BTC'}
        result = {}
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                kraken_account_balance = pykraken.kprivate.kprivate_balance(self._kraken_client)
                for currency in kraken_account_balance:
                    kraken_currency = currency
                    if kraken_currency in kraken_symbols_dict:
                        kraken_currency = kraken_symbols_dict[currency]
                    result[kraken_currency] = {"amount": float(kraken_account_balance[currency]),
                                               "available": float(kraken_account_balance[currency])}
            except Exception as e:
                self.log.error("%s", str(e))

            if "USD" not in result:
                result["USD"] = {'amount': 0, 'available': 0}
        return result

    def get_exchange_name(self):
        return "Kraken"

    def _execute_exchange_order(self, action_type, cancel, size, price, crypto_type):
        self.log.debug("Executing <%s>, size=<%f>, price=<%f>, type=<%s>, cancel=<%s>", action_type, size,
                       price, crypto_type, cancel)
        print("Executing <{}>, size=<{}>, price=<{}>, type=<{}>, cancel=<{}>".format(action_type, size, price,
                                                                                     crypto_type,
                                                                                     cancel))
        asset_pair = crypto_type.upper() + "-USD"
        kraken_pair = asset_pair
        if kraken_pair in KrakenOrderbook.KRAKEN_PAIRS_DICT:
            kraken_pair = KrakenOrderbook.KRAKEN_PAIRS_DICT[asset_pair]
        execute_result = {'exchange': self.get_exchange_name(),
                          'order_status': False,
                          'executed_price_usd': price}
        try:
            if self._kraken_client is not None and self._signed_in_user != "":
                exchange_order = pykraken.kprivate.kprivate_addorder(self._kraken_client, kraken_pair, action_type,
                                                                     'limit', price, None, size)
                execute_result['id'] = exchange_order['txid'][0]
                if self._cancel_order(execute_result['id']):
                    execute_result['status'] = "Cancelled"
                else:
                    self.log.debug("Can't cancel order <%s>, order done", execute_result['id'])
                    execute_result['order_status'] = True
                    execute_result['status'] = 'Finished'
                    print("Order finished")
                    exchange_order_status = self.order_status(execute_result['id'])
                    if execute_result['id'] not in exchange_order_status:
                        # We don't know the price so we set the limit price as a speculation
                        execute_result['executed_price_usd'] = price
                    else:
                        execute_result['executed_price_usd'] = \
                            float(exchange_order_status[execute_result['id']]['price'])
                    print("Kraken done")

        except Exception as e:
            self.log.error("%s %s", action_type, e)
            print("kraken error:", e)
            execute_result['status'] = 'Error'
            execute_result['order_status'] = False
        return execute_result

    def buy_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("buy", True, execute_size_coin, price_fiat, crypto_type)

    def sell_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("sell", True, execute_size_coin, price_fiat, crypto_type)

    def order_status(self, order_id):
        result = {}
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                result = pykraken.kprivate.kprivate_queryorders(self._kraken_client, txid=[order_id])
            except Exception as e:
                self.log.error("%s", str(e))
        return result

    def transactions(self, transactions_limit):
        transactions = dict()
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                transactions = pykraken.kprivate.kprivate_tradeshistory(self._kraken_client)
            except Exception as e:
                self.log.error("%s", str(e))
        return transactions

    def exchange_fee(self, crypto_type):
        return 0.2

    def minimum_order_size(self, asset_pair):
        minimum_sizes = {'BTC-USD': 0.002, 'BCH-USD': 0.02}
        return minimum_sizes[asset_pair]

    def buy_limit(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("buy", False, execute_size_coin, price_fiat, crypto_type)

    def sell_limit(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("sell", False, execute_size_coin, price_fiat, crypto_type)

    def create_order_tracker(self, order, orderbook, order_info):
        return KrakenOrderTracker(order, orderbook, self, order_info)

    def _cancel_order(self, order_id):
        cancel_status = False
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                cancel_status = pykraken.kprivate.kprivate_cancelorder(self._kraken_client, order_id)
                self.log.debug("Cancel status: <%s>", cancel_status)
                print("Cancel status:", cancel_status)
            except Exception as e:
                self.log.error("Cancel exception: %s", str(e))
                print("Kraken cancel error:", e, cancel_status)
        return cancel_status

    def exchange_accuracy(self):
        return '1e-1'
