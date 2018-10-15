import bitfinex
import client_wrapper_base
import logging
from order_tracker import BitfinexOrderTracker

class BitfinexClientWrapper(client_wrapper_base.ClientWrapperBase):
    def __init__(self, credentials, orderbook, db_interface, clients_manager):
        super().__init__(orderbook, db_interface, clients_manager)
        self.log = logging.getLogger(__name__)
        self._bitfinex_client = None
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
                print("Bitfinex account", bitfinex_account_balance)
                if 'error' in bitfinex_account_balance and \
                        bitfinex_account_balance['error'] == "ERR_RATE_LIMIT" or \
                        'message' in bitfinex_account_balance and \
                        bitfinex_account_balance['message'] == 'Nonce is too small.':
                    result['error'] = "ERR_RATE_LIMIT"
                else:
                    for curr_balance in bitfinex_account_balance:
                        currency = curr_balance['currency']
                        result[currency.upper()] = {"amount": float(curr_balance['amount']),
                                                    "available": float(curr_balance['available'])}
            except Exception as e:
                self.log.error("%s", str(e))
                print("Bitfinex account error:", e)

            if "USD" not in result:
                result["USD"] = {'amount': 0, 'available': 0}
        return result

    def get_exchange_name(self):
        return "Bitfinex"

    def _execute_exchange_order(self, action_type, size, price, currency_to, exchange_instruction, currency_from = 'USD'):
        self.log.info("Executing <%s>, size=<%f>, price=<%f>, type=<%s>, exchange_instruction=<%s>", action_type, size,
                       price, currency_to, exchange_instruction)
        print("Executing <{}>, size=<{}>, price=<{}>, type=<{}>".format(action_type, size, price, currency_to,
                                                                        exchange_instruction))
        execute_result = {'order_status': False}
        try:
            if self._bitfinex_client is not None and self._signed_in_user != "":
                exchange_result = self._bitfinex_client.place_order(str(size), str(price), action_type,
                                                                    exchange_instruction, currency_to.lower() + currency_from.lower())
                if 'id' in exchange_result:
                    exchange_status = self.order_status(exchange_result['id'])
                else:
                    raise Exception (exchange_result)                                                    
                #print("Bitfinex status:", exchange_status)
                execute_result = {'exchange': self.get_exchange_name(),
                                  'id': int(exchange_result['id']),
                                  'executed_price_usd': exchange_status['avg_execution_price'],
                                  'order_status': False}
                if exchange_status['is_cancelled'] or exchange_status['avg_execution_price'] == 0:
                    execute_result['status'] = "Cancelled"
                else:
                    execute_result['status'] = 'Finished'
                    execute_result['order_status'] = True
                #print(execute_result)
        except Exception as e:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -->action_type = " + action_type + " e = " + e)
            self.log.error("action_type = %s, e =  %s", action_type, e)
            execute_result['status'] = 'Error'
            execute_result['order_status'] = True
        return execute_result

    def buy_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("buy", execute_size_coin, price_fiat, crypto_type, "exchange fill-or-kill")

    def sell_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("sell", execute_size_coin, price_fiat, crypto_type, "exchange fill-or-kill")

    def order_status(self, order_id):
        result = {}
        if self._bitfinex_client is not None and self._signed_in_user != "":
            try:
                result = self._bitfinex_client.status_order(order_id)
            except Exception as e:
                self.log.error("%s", str(e))
        return result

    def transactions(self, transactions_limit):
        transactions = []
        exchange_asset_pairs = self._orderbook.get_asset_pairs()
        for asset_pair_key in exchange_asset_pairs:
            try:
                asset_pair = exchange_asset_pairs[asset_pair_key].lower()
                transactions = transactions + self._bitfinex_client.past_trades(0, asset_pair)
            except Exception as e:
                self.log.error("%s", str(e))
        return transactions

    def exchange_fee(self, crypto_type):
        return 0.2

    def minimum_order_size(self, asset_pair):
        minimum_sizes = {'BTC-USD': 0.002, 'BCH-USD': 0.02}
        return minimum_sizes[asset_pair]

    def buy_limit(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("buy", execute_size_coin, price_fiat, crypto_type, "exchange limit")

    def sell_limit(self, execute_size_coin, price_fiat, crypto_type):
        return self._execute_exchange_order("sell", execute_size_coin, price_fiat, crypto_type, "exchange limit")

    def create_order_tracker(self, order, orderbook, order_info, crypto_type):
        order['id'] = int(order['id'])
        return BitfinexOrderTracker(order, orderbook, self, order_info, crypto_type)

    def exchange_accuracy(self):
        return '1e-1'

    def _cancel_order(self, order_id):
        cancel_status = False
        if self._bitfinex_client is not None and self._signed_in_user != "":
            try:
                cancel_status = self._bitfinex_client.delete_order(order_id)
                self.log.debug("Cancel status: <%s>", cancel_status)
            except Exception as e:
                self.log.error("Cancel exception: %s", str(e))
        return cancel_status


    def sell_market(self, execute_size_coin, currency_from , currency_to):
        return self._execute_exchange_order("sell", execute_size_coin, 0.01, currency_to, "market", currency_from)    

    def buy_market(self, execute_size_coin, currency_from, currency_to):
        return self._execute_exchange_order("buy", execute_size_coin, 0.01, currency_to, "market", currency_from)