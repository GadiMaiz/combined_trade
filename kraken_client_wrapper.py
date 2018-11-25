import pykraken
import client_wrapper_base
import logging
import pykraken.kprivate
from kraken_orderbook import KrakenOrderbook
from order_tracker import KrakenOrderTracker


class KrakenClientWrapper(client_wrapper_base.ClientWrapperBase):
    def __init__(self, credentials, orderbook, db_interface, clients_manager):
        super().__init__(orderbook, db_interface, clients_manager)
        self.log = logging.getLogger('smart-trader')
        self._kraken_client = None
        self._signed_in_user = ""
        self.set_credentials(credentials)

    def set_credentials(self, client_credentials, cancel_order=True):
        super().set_credentials(client_credentials)
        self._should_have_balance = False
        username = ''
        key = ''
        secret = ''
        if cancel_order:
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
        kraken_symbols_dict = {'ZUSD': 'USD', 'XXBT': 'BTC', 'ZEUR': 'EUR', 'XLTC': 'LTC', 'XETH': 'ETH'}
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
                result['error'] = str(e)
            if "USD" not in result:
                result["USD"] = {'amount': 0, 'available': 0}
        return result

    def get_exchange_name(self):
        return "Kraken"

    def _execute_exchange_order(self, action, cancel, size, currency_from, currency_to, price=None):
        self.log.debug("Executing <%s>, size=<%f>, price=<%f>, type_from=<%s>, type_to=<%s>, cancel=<%s>",
                       action, size,  price, currency_from, currency_to, cancel)

        action_type_parsed = action.split('_')
        buy_or_sell = action_type_parsed[0]
        action_type = action_type_parsed[1]
        asset_pair = currency_to.upper() + "-" + currency_from.upper()
        kraken_pair = asset_pair
        if kraken_pair in KrakenOrderbook.KRAKEN_PAIRS_DICT:
            kraken_pair = KrakenOrderbook.KRAKEN_PAIRS_DICT[asset_pair]
            self.log.debug("Trading in kraken pair: <%s>", kraken_pair)
        execute_result = {'exchange': self.get_exchange_name(),
                          'order_status': False,
                          'executed_price_usd': price}
        try:
            if self._kraken_client is not None and self._signed_in_user != "":
                exchange_order = pykraken.kprivate.kprivate_addorder(self._kraken_client, kraken_pair, buy_or_sell,
                                                                     action_type, price, None, size)
                execute_result['id'] = exchange_order['txid'][0]
                if not cancel:
                    self.log.debug("Not cancelling order <%s>", execute_result['id'])
                    exchange_order_status = self.order_status(execute_result['id'])
                    if execute_result['id'] not in exchange_order_status:
                        # We don't know the price so we set the limit price as a speculation
                        self.log.debug("Price not in status <%s>", exchange_order_status)
                        execute_result['executed_price_usd'] = price
                    else:
                        self.log.debug("Price in status <%s>", exchange_order_status)
                        execute_result['executed_price_usd'] = \
                            float(exchange_order_status[execute_result['id']]['price'])
                    if exchange_order_status[execute_result['id']]['status'] == 'open':
                        self.log.info("Status is open")
                        execute_result['status'] = "Open"
                    elif exchange_order_status[execute_result['id']]['status'] == 'closed':
                        self.log.info("Status is closed, finishing order")
                        execute_result['status'] = "Finished"
                    else:
                        self.log.error("Unknown order status: <%s>", exchange_order_status)
                        execute_result['status'] = "Error"
                else:
                    if self._cancel_order(execute_result['id'], False):
                        self.log.error("order <%s> was Cancelled, order execution failed", execute_result['id'])
                        execute_result['status'] = "Cancelled"
                    else:
                        execute_result['order_status'] = True
                        execute_result['status'] = 'Finished'
                        self.log.info("order <%s> finished successfully", execute_result['id'])
                        exchange_order_status = self.order_status(execute_result['id'])
                        if execute_result['id'] not in exchange_order_status:
                            # We don't know the price so we set the limit price as a speculation
                            execute_result['executed_price_usd'] = price
                        else:
                            execute_result['executed_price_usd'] = \
                                float(exchange_order_status[execute_result['id']]['price'])
                self.log.debug("Kraken done")

        except Exception as e:
            self.log.error("%s %s %s", action_type, kraken_pair, e)
            execute_result['status'] = 'Error'
            execute_result['order_status'] = False
            exception_str = str(e)
            execute_result['execution_message'] = "{} {}".format(action_type, exception_str[0:min(
                100, len(exception_str))])
        return execute_result

    def buy_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_exchange_order("buy_limit", True, execute_size_coin, currency_from, currency_to, price)

    def sell_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_exchange_order("sell_limit", True, execute_size_coin, currency_from, currency_to, price)

    def sell_market(self, execute_size_coin, currency_from, currency_to):
        return self._execute_exchange_order(action="sell_market", cancel=True, size=execute_size_coin,
                                            currency_from=currency_from, currency_to=currency_to)

    def buy_market(self, execute_size_coin, currency_from, currency_to):
        return self._execute_exchange_order(action="buy_market", cancel=True, size=execute_size_coin,
                                            currency_from=currency_from, currency_to=currency_to)

    def order_status(self, order_id):
        result = {}
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                result = pykraken.kprivate.kprivate_queryorders(self._kraken_client, txid=[order_id])
            except Exception as e:
                self.log.error("%s", str(e))
                print("Order status error:", e)
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

    def buy_limit(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_exchange_order("buy_limit", False, execute_size_coin, currency_from, currency_to, price)

    def sell_limit(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_exchange_order("sell_limit", False, execute_size_coin, currency_from, currency_to, price)

    def create_order_tracker(self, order, orderbook, order_info, currency_from, currency_to):
        return KrakenOrderTracker(order, orderbook, self, order_info, currency_from, currency_to)

    def _cancel_order(self, order_id, expect_to_be_cancelled=True):
        cancel_status = False
        if self._kraken_client is not None and self._signed_in_user != "":
            try:
                cancel_status = pykraken.kprivate.kprivate_cancelorder(self._kraken_client, order_id)
                self.log.debug("Cancel status: <%s>", cancel_status)
            except Exception as e:
                if expect_to_be_cancelled:
                    self.log.error("Cancel exception: %s", str(e))
                else:
                    self.log.debug("Cancel exception: %s", str(e))
        return cancel_status

    def exchange_accuracy(self):
        return '1e-1'
