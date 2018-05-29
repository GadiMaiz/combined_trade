import bitstamp.client
import client_wrapper_base
import logging

class BitstampClientWrapper(client_wrapper_base):
    def __init__(self, bitstamp_credentials, bitstamp_orderbook, db_file):
        super().__init__(bitstamp_orderbook, db_file)
        self.log = logging.getLogger(__name__)
        self._bitstamp_client = None
        self._signed_in_user = ""
        self.set_client_credentails(bitstamp_credentials)

    def _get_available_account_balance(self, crypto_type):
        account_balance = self.account_balance(crypto_type)
        crypto_key = ''
        for curr_account_balance_key in account_balance:
            if curr_account_balance_key.endswith("_available") and not curr_account_balance_key.startswith("usd"):
                crypto_key = curr_account_balance_key
                break

        if crypto_key != '':
            account_balance['crypto_available'] = account_balance[crypto_key]
            account_balance['crypto_type'] = crypto_type
            account_balance['exchange'] = "Bitstamp"
        return account_balance

    def set_client_credentails(self, client_credentials):
        username = ''
        key = ''
        secret = ''
        self.super().cancel_timed_order()
        try:
            if client_credentials is not None and 'username' in client_credentials and \
                    'key' in client_credentials and 'secret' in client_credentials:
                username = client_credentials['username']
                key = client_credentials['key']
                secret = client_credentials['secret']

            if len(username) != 0 and len(username) != 0 and len(username) != 0:
                self._bitstamp_client = bitstamp.client.Trading(username=username, key=key, secret=secret)
                self._bitstamp_client.account_balance('BTC')
                self._signed_in_user = username
                self._balance_changed = {}
            else:
                self._signed_in_user = ''
                self._bitstamp_client = None
        except:
            self._bitstamp_client = None
            self._signed_in_user = ''
            self._balance_changed = {}

        return self._bitstamp_client is not None

    def get_signed_in_credentials(self):
        signed_in_dict = {True: "True", False: "False"}
        return {'signed_in_user': self._signed_in_user, 'is_user_signed_in': signed_in_dict[self._signed_in_user != ""]}

    def logout(self):
        self.set_client_credentails({})
        return self._bitstamp_client is None

    def _get_balance_from_exchange(self, crypto_type):
        result = []
        if self._bitstamp_client is not None and self._signed_in_user != "":
            try:
                bitstamp_account_balance = self._bitstamp_client.account_balance(crypto_type)
                crypto_bistamp_key = crypto_type.lower() + "_available"
                if bitstamp_account_balance is not None and crypto_bistamp_key in bitstamp_account_balance and \
                   "usd_available" in bitstamp_account_balance:
                    result.append({"type": crypto_type, "amount": bitstamp_account_balance[crypto_bistamp_key]})
                    result.append({"type": 'USD', "amount": bitstamp_account_balance["usd_available"]})
            except Exception as e:
                self.log.error("%s", str(e))
        return result

    def get_exchange_name(self):
        return "Bitstamp"

    def _execute_exchange_order(self, exchange_method, size, price, crypto_type):
        execute_result = {}
        try:
            if self._bitstamp_client is not None and self._signed_in_user != "":
                execute_result = exchange_method(size, price, crypto_type)
        except Exception as e:
            self.log.error("%s %s", str(type(exchange_method)), str(e))
        return execute_result

    def buy_limit_order(self, execute_size_coin, price_fiat, crypto_type):
        buy_result = {}
        if self._bitstamp_client is not None and self._signed_in_user != "":
            buy_result = self._execute_exchange_order(self._bitstamp_client.buy_limit_order, execute_size_coin,
                                                      price_fiat, crypto_type)
        return buy_result

    def sell_limit_order(self, execute_size_coin, price_fiat, crypto_type):
        sell_result = {}
        if self._bitstamp_client is not None and self._signed_in_user != "":
            sell_result = self._execute_exchange_order(self._bitstamp_client.buy_limit_order, execute_size_coin,
                                                      price_fiat, crypto_type)
        return sell_result

    def order_status(self, order_id):
        order_status = {}
        if self._bitstamp_client is not None and self._signed_in_user != "":
            try:
                order_status = self._bitstamp_client.order_status(order_id)
            except Exception as e:
                self.log.error("%s", str(e))
        return order_status

    def cancel_order(self, order_id):
        cancel_status = {}
        if self._bitstamp_client is not None and self._signed_in_user != "":
            try:
                cancel_status = self._bitstamp_client.cancel_order(order_id)
            except Exception as e:
                self.log.error("%s", str(e))
        return cancel_status

    def transactions(self, transactions_limit):
        transactions = []
        try:
            if self._bitstamp_client is not None and self._signed_in_user != "":
                self._transactions = self._bitstamp_client.user_transactions()
                transactions = self._transactions
                if transactions_limit != 0 and len(transactions) > transactions_limit:
                    transactions = transactions[0:transactions_limit]
        except Exception as e:
            self.log.error("%s", str(e))
            transactions = []
        return transactions