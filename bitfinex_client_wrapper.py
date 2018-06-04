import bitfinex
import client_wrapper_base
import logging

class BitfinexClientWrapper(client_wrapper_base.ClientWrapperBase):
    def __init__(self, bitfinex_credentials, bitfinex_orderbook, db_interface):
        super().__init__(bitfinex_orderbook, db_interface)
        self.log = logging.getLogger(__name__)
        self._bitfinex_client = None
        self._signed_in_user = ""
        self.set_credentials(bitfinex_credentials)

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

            if len(username) != 0 and len(username) != 0 and len(username) != 0:
                self._bitfinex_client = bitfinex.TradeClient(key=key, secret=secret)
                self._bitfinex_client.balances()
                self._signed_in_user = username
                self._balance_changed = True
                self._is_client_init = True
            else:
                self._signed_in_user = ''
                self._bitfinex_client = None
        except Exception as e:
            self.log.error("Sign in exception: {}".format(e))
            self._bitfinex_client = None
            self._signed_in_user = ''
            self._balance_changed = False
            self._is_client_init = False

        return self._bitfinex_client is not None

    def get_signed_in_credentials(self):
        signed_in_dict = {True: "True", False: "False"}
        return {'signed_in_user': self._signed_in_user, 'is_user_signed_in': signed_in_dict[self._signed_in_user != ""]}

    def logout(self):
        super().logout()
        return self._bitfinex_client is None

    def _get_balance_from_exchange(self):
        result = {}
        if self._bitfinex_client is not None and self._signed_in_user != "":
            try:
                bitfinex_account_balance = self._bitfinex_client.balances()
                for curr_balance in bitfinex_account_balance:
                    currency = curr_balance['currency']
                    result[currency.upper()] = {"amount": float(curr_balance['amount']),
                                                "available": float(curr_balance['available'])}
            except Exception as e:
                self.log.error("%s", str(e))

            if "USD" not in result:
                result["USD"] = {'amount': 0, 'available': 0}
        return result

    def get_exchange_name(self):
        return "Bitfinex"

    def _execute_exchange_order(self, action_type, size, price, crypto_type):
        self.log.debug("Executing <%s>, size=<%f>, price=<%f>, type=<%s>", action_type, size, price, crypto_type)
        print("Executing <{}>, size=<{}>, price=<{}>, type=<{}>".format(action_type, size, price, crypto_type))
        execute_result = {'order_status': False}
        #try:
        if self._bitfinex_client is not None and self._signed_in_user != "":
            exchange_result = self._bitfinex_client.place_order(str(size), str(price), action_type,
                                                       "exchange fill-or-kill", crypto_type.lower() + "usd")
            print(exchange_result)
            exchange_status = self._bitfinex_client.status_order(exchange_result['id'])
            execute_result = {'exchange': self.get_exchange_name(),
                              'id': exchange_result['id'],
                              'executed_price_usd': exchange_status['avg_execution_price']}
            if exchange_status['is_cancelled']:
                execute_result['status'] = "Cancelled"
            else:
                execute_result['status'] = 'Finished'
                execute_result['order_status'] = True
        #except Exception as e:
        #    self.log.error("%s %s", action_type, e)
        return execute_result

    def buy_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("buy", execute_size_coin, price_fiat, crypto_type)

    def sell_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("sell", execute_size_coin, price_fiat, crypto_type)

    def order_status(self, order_id):
        order_status = {}
        if self._bitfinex is not None and self._signed_in_user != "":
            try:
                order_status = self._bitfinex.order_status(order_id)
            except Exception as e:
                self.log.error("%s", str(e))
        return order_status

    def transactions(self, transactions_limit):
        transactions = []
        exchange_asset_pairs = self._orderbook.get_assets_pair()
        for asset_pair_key in exchange_asset_pairs:
            try:
                asset_pair = exchange_asset_pairs[asset_pair_key].lower()
                transactions = transactions + self._bitfinex_client.past_trades(0, asset_pair)
            except Exception as e:
                self.log.error("%s", str(e))
        return transactions

    def exchange_fee(self, crypto_type):
        return 0.2

    def minimum_order_size(self, crypto_type):
        minimum_sizes = {'BTC': 0.002, 'BCH': 0.02}
        return minimum_sizes[crypto_type]