import bitstamp.client
import client_wrapper_base
import logging
import time
from order_tracker import BitstampOrderTracker


class BitstampClientWrapper(client_wrapper_base.ClientWrapperBase):
    def __init__(self, credentials, orderbook, db_interface, clients_manager, account):
        super().__init__(orderbook, db_interface, clients_manager, account)
        self.log = logging.getLogger('smart-trader')
        self._bitstamp_client = None
        self._signed_in_user = ""
        self._api_key = ""
        self._secret = ""
        self.set_credentials(credentials)
        self._fee = 0

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

            if len(username) != 0 and len(username) != 0 and len(username) != 0:
                self._bitstamp_client = bitstamp.client.Trading(username=username, key=key, secret=secret)
                self._bitstamp_client.account_balance("","")
                self._signed_in_user = username
                self._api_key = key
                self._secret = secret
                self._balance_changed = True
                self._is_client_init = True
            else:
                self._signed_in_user = ''
                self._bitstamp_client = None
        except Exception as e:
            self.log.error("Login exception: <%s>", e)
            self._bitstamp_client = None
            self._signed_in_user = ''
            self._balance_changed = False
            self._is_client_init = False

        return self._bitstamp_client is not None

    def get_signed_in_credentials(self):
        signed_in_dict = {True: "True", False: "False"}
        return {'signed_in_user': self._signed_in_user, 'is_user_signed_in': signed_in_dict[self._signed_in_user != ""]}

    def logout(self):
        super().logout()
        return self._bitstamp_client is None

    def _get_balance_from_exchange(self):
        result = {}
        if self._bitstamp_client is not None and self._signed_in_user != "":
            try:
                bitstamp_account_balance = self._bitstamp_client.account_balance(False, False)
                fees = dict()
                if 'btcusd_fee' in bitstamp_account_balance:
                    self._fee = float(bitstamp_account_balance['btcusd_fee'])
                elif 'fee' in bitstamp_account_balance:
                    self._fee = float(bitstamp_account_balance['fee'])
                for bitstamp_balance_key in bitstamp_account_balance:
                    if bitstamp_balance_key.endswith("_available"):
                        available_balance = float(bitstamp_account_balance[bitstamp_balance_key])
                        balance_key = bitstamp_balance_key.replace("_available", "_balance")
                        balance = 0
                        if balance_key in bitstamp_account_balance:
                            balance = float(bitstamp_account_balance[balance_key])
                        currency = bitstamp_balance_key.replace("_available", "")
                        result[currency.upper()] = {"amount": balance, "available": available_balance}
                    """elif bitstamp_balance_key.endswith("usd_fee"):
                        fees[bitstamp_balance_key.replace("usd_fee", "")] = \
                            float(bitstamp_account_balance[bitstamp_balance_key])
                self._orderbook['fees'].update(fees)
                self._orderbook['orderbook'].set_fees(self._orderbook['fees'])"""
            except Exception as e:
                self.log.error("Exception: <%s>", str(e))
        return result

    def get_exchange_name(self):
        return "Bitstamp"

    def _execute_immediate_or_cancel(self, exchange_method, size, price, currency_from, currency_to, cancel_not_done):
        self.log.debug("Executing <%s>, size=<%f>, price=<%f>, type_from=<%s>, type_to=<%s> ", exchange_method, size,
                       price, currency_from, currency_to)
        execute_result = {'exchange': self.get_exchange_name(), 'order_status': False, 'executed_price_usd': price,
                          'status': 'Init'}
        try:
            if self._bitstamp_client is not None and self._signed_in_user != "":
                if price is None:
                    limit_order_result = exchange_method(size, currency_from.lower(), currency_to.lower())
                else:
                    limit_order_result = exchange_method(size, price, currency_to.lower(), currency_from.lower())
                        
                self.log.info("Execution result: <%s>", execute_result)
                order_id = limit_order_result['id']
                execute_result['id'] = int(order_id)
                execute_result['executed_price_usd'] = price
                order_status = self.order_status(order_id)
                self.log.debug("order status <%s>", order_status)
                cancel_status = False
                if order_status is not None and 'status' in order_status and order_status['status'] == 'Finished' and \
                        len(order_status['transactions']) > 0:
                    execute_result['status'] = 'Finished'
                    execute_result['executed_price_usd'] = order_status['transactions'][0]['price']
                    execute_result['order_status'] = True
                elif order_status is None:
                    execute_result['status'] = 'Finished'
                    execute_result['order_status'] = True
                elif cancel_not_done:
                    self.log.debug("Cancelling order <%d>", order_id)
                    if order_status is not None:
                        cancel_status = self._cancel_order(order_id)
                        if cancel_status:
                                execute_result['status'] = 'Cancelled'
                                self.log.info("Order <%d> cancelled", order_id)

                if not cancel_status and not cancel_not_done:
                    execute_result['status'] = 'Open'
                    execute_result['order_status'] = True
                elif not cancel_status:
                    execute_result['status'] = 'Finished'
                    try:
                        found_transaction = False
                        all_transactions = self._bitstamp_client.user_transactions()
                        asset_pair_key = currency_to.lower() + "_" + currency_from.lower()
                        self.log.debug("curr transaction <%s> transactions: <%s>", order_id, all_transactions)
                        for curr_transaction in all_transactions:
                            if curr_transaction['order_id'] == order_id or curr_transaction['order_id'] == int(order_id):
                                execute_result['executed_price_usd'] = curr_transaction[asset_pair_key]
                                found_transaction = True
                                break
                        if not found_transaction:
                            self.log.warning("Transaction for <%d> not found", order_id)
                        execute_result['order_status'] = True
                    except Exception as e:
                        self.log.error("Exception while getting transactions data: <%s>", str(e))

        except Exception as e:
            self.log.error("Error executing %s, size=<%f>, price=<%f>, type_from=<%s>, type_to=<%s>, error is: <%s> ",
                           exchange_method, size, price, currency_from, currency_to, str(e))
            execute_result['status'] = 'Error'
        return execute_result

    def buy_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_immediate_or_cancel(self._bitstamp_client.buy_limit_order, execute_size_coin, price,
                                                 currency_from, currency_to, True)

    def sell_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_immediate_or_cancel(self._bitstamp_client.sell_limit_order, execute_size_coin, price,
                                                 currency_from, currency_to, True)

    def order_status(self, order_id):
        order_status = None
        if self._bitstamp_client is not None and self._signed_in_user != "":
            try:
                order_status = self._bitstamp_client.order_status(order_id)
            except Exception as e:
                self.log.error("can't get order status for order <%s>: %s", order_id, str(e))
        return order_status

    def _cancel_order(self, order_id, expect_to_be_cancelled=True):
        cancel_status = False
        if self._bitstamp_client is not None and self._signed_in_user != "":
            internal_error_exception = True
            while internal_error_exception:
                internal_error_exception = False
                try:
                    cancel_status = self._bitstamp_client.cancel_order(order_id)
                    self.log.debug("Cancel status: <%s>", cancel_status)
                except Exception as e:
                    self.log.debug("Cancel exception: %s", str(e))
                    if "INTERNAL SERVER ERROR" in str(e):
                        self.log.error("Can't cancel because of internal server error exception: %s", str(e))
                        internal_error_exception = True
                        time.sleep(0.5)
        return cancel_status

    def transactions(self, transactions_limit):
        transactions = []
        try:
            if self._bitstamp_client is not None and self._signed_in_user != "":
                transactions = self._bitstamp_client.user_transactions()
                if transactions_limit != 0 and len(transactions) > transactions_limit:
                    transactions = transactions[0:transactions_limit]
        except Exception as e:
            self.log.error("%s", str(e))
            transactions = []
        return transactions

    def exchange_fee(self, crypto_type):
        return self._fee

    def buy_limit(self, execute_size_coin, price, currency_from, currency_to):
        self.reconnect()
        if self._bitstamp_client is not None and self._signed_in_user != "":
            result = self._execute_immediate_or_cancel(self._bitstamp_client.buy_limit_order, execute_size_coin,
                                                       price, currency_from, currency_to, False)
        else:
            result = {'exchange': self.get_exchange_name(), 'order_status': False, 'status': 'Error'}
        return result

    def sell_limit(self, execute_size_coin, price, currency_from, currency_to):
        self.reconnect()
        if self._bitstamp_client is not None and self._signed_in_user != "":
            result = self._execute_immediate_or_cancel(self._bitstamp_client.sell_limit_order, execute_size_coin,
                                                       price, currency_from, currency_to, False)
        else:
            result = {'exchange': self.get_exchange_name(), 'order_status': False, 'status': 'Error'}
        return result

    def get_order_status_from_transactions(self, order_id, currency_from, currency_to):
        results = {'executed_size': 0, 'transactions': []}
        all_transactions = self.transactions(500)
        self.log.debug("curr transaction <%d> transactions: <%s>", order_id, all_transactions)
        for curr_transaction in all_transactions:
            if 'order_id' in curr_transaction and int(curr_transaction['order_id']) == int(order_id):
                results['executed_size'] += abs(float(curr_transaction[(currency_to.lower())]))
                results['transactions'].append(curr_transaction)
                self.log.debug("update from transactions result: <%s>", results)
        return results

    def create_order_tracker(self, order, orderbook, order_info, currency_from, currency_to):
        return BitstampOrderTracker(order, orderbook, self, order_info, currency_from, currency_to,
                                    self._timed_orders[currency_to])

    def reconnect(self):
        if self._bitstamp_client is None and self._signed_in_user != "":
            self.set_credentials({'username': self._signed_in_user, 'key': self._api_key, 'secret': self._secret})


    def sell_market(self, size, currency_from, currency_to):
        return self._execute_immediate_or_cancel(self._bitstamp_client.sell_market_order, size, None, currency_from, False, currency_to)

    def buy_market(self, size, currency_to, currency_from):
        return self._execute_immediate_or_cancel(self._bitstamp_client.buy_market_order, size, None, currency_from, False, currency_to)
