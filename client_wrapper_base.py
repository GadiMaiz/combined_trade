import datetime
import time
from threading import Thread
import random
import math
from timed_order_executer import TimedOrderExecuter
from decimal import Decimal
import logging
import traceback
import sys

class ClientWrapperBase:
    TIMED_EXECUTION_SLEEP_SEC = 0.5
    EXECUTED_ORDER_MIN_DELAY_SEC = 2
    EXECUTED_ORDER_MAX_DELAY_SEC = 8
    ORDER_EXECUTION_MIN_FACTOR = 0.3
    ORDER_EXECUTION_MAX_FACTOR = 0.7
    MAX_EXECUTION_MIN_FACTOR = 0.6
    MAX_EXECUTION_MAX_FACTOR = 1
    RELATIVE_RANGE_FOR_EXECUTION_START = 0.001
    TIMED_ORDERS_DICT = { True : 1, False : 0}
    CRYPTO_CURRENCIES_DICT = {'BTC': 'btc', 'BCH': 'bch'}

    def __init__(self, orderbook, db_interface, clients_manager):
        self.log = logging.getLogger(__name__)
        self._timed_order_thread = None
        self._last_balance = {}
        self._transactions = []
        self._is_timed_order_running = False
        self._timed_order_action = ''
        self._timed_order_price_fiat = 0
        self._timed_order_start_time = ''
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = 0
        self._timed_order_done_size = 0
        self._orderbook = orderbook
        self._balance_changed = False
        self._reserved_crypto = 0
        self._reserved_crypto_type = ""
        self._reserved_usd = 0
        self._timed_order_elapsed_time = 0
        self._timed_order_duration_sec = 0
        self._signed_in_user = ""
        self._is_client_init = False
        self._db_interface = db_interface
        self._clients_manager = clients_manager

    def account_balance(self):
        if self._is_client_init and self._balance_changed:
            try:
                self._last_balance['balances'] = self._get_balance_from_exchange()
                for balance in self._last_balance['balances']:
                    if balance != "USD":
                        self._last_balance['balances'][balance]['price'] = self._get_pair_price(balance)

                for crypto_type_key in ClientWrapperBase.CRYPTO_CURRENCIES_DICT:
                    if crypto_type_key not in self._last_balance['balances']:
                        self._last_balance['balances'][crypto_type_key] = {'amount': 0.0, 'available': 0.0,
                                                                           'price': 0.0}

                self._balance_changed = False
            except Exception as e:
                self.log.error(e)
        self._last_balance['reserved_crypto'] = self._reserved_crypto
        self._last_balance['reserved_crypto_type'] = self._reserved_crypto_type
        self._last_balance['server_usd_reserved'] = self._reserved_usd
        self._last_balance['fee'] = self.exchange_fee("BTC")
        return self._last_balance

    def send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        order_sent = {'order_status': False, 'execution_size': 0, 'execution_message': ''}
        if not self.is_client_initialized():
            order_sent['execution_message'] = 'Exchange client not initialized'
        else:
            order_allowed = self.can_send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec)
            if not order_allowed['can_send_order']:
                order_sent['execution_message'] = order_allowed['reason']
                self.log.warning("Order not allowed: <%s>", order_allowed['reason'])
            else:
                if duration_sec == 0:
                    order_sent = self.send_immediate_order(action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                                           False, 0)
                    self._order_complete(False)
                else:
                    actions_dict = {'timed_sell': 'sell', 'timed_buy': 'buy'}
                    self.log.info("Time order, action: <%s>, size_coin: <%f>, crypto_type: <%s>, price: <%f> "
                                  "fiat_type: <%s>, duration_sec: <%f>, max_order_size: <%f>",
                                  actions_dict[action_type], size_coin, crypto_type, price_fiat, fiat_type,
                                  duration_sec, max_order_size)
                    if not self.is_timed_order_running():
                        order_sent = self.execute_timed_order(actions_dict[action_type], size_coin, crypto_type,
                                                              price_fiat, fiat_type, duration_sec, max_order_size)
                    else:
                        self.log.warning("Timed order already running, ignoring new timed order")
        return order_sent

    def can_send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec):
        result, refuse_reason = ClientWrapperBase.verify_order_params(size_coin, price_fiat, duration_sec)

        if result:
            balance_before_order = self.account_balance()
            if result and action_type == 'sell' and size_coin > \
                    balance_before_order['balances'][crypto_type]['available']:
                refuse_reason = "Available balance " + \
                                str(balance_before_order['balances'][crypto_type]['available']) + \
                                crypto_type + " is less than required size " + str(size_coin) + crypto_type
                result = False
            elif result and action_type == 'buy' and (price_fiat * size_coin * (1 + 0.01 *
                                                                                balance_before_order['fee'])) > \
                    float(balance_before_order['balances'][fiat_type]['available']):
                refuse_reason = "Available balance " + str(balance_before_order['balances'][fiat_type]['available']) + \
                                "USD is less than required balance " + \
                                str(price_fiat * size_coin * (1 + 0.01 * balance_before_order['fee']))
                result = False
        return {'can_send_order': result, 'reason': refuse_reason}

    @staticmethod
    def verify_order_params(size_coin, price_fiat, duration_sec):
        result = True
        refuse_reason = ''
        try:
            refuse_reason = "Invalid size"
            if float(size_coin) <= 0:
                result = False

            if result:
                refuse_reason = "Invalid price"
                price_fiat = float(price_fiat)
                if float(price_fiat) <= 0:
                    result = False

            if result:
                refuse_reason = "Invalid duration"
                duration_sec = float(duration_sec)
                if duration_sec < 0:
                    result = False

            if result:
                refuse_reason = ""
        except ValueError:
            result = False

        return result, refuse_reason

    def execute_timed_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        if self._timed_order_thread is not None and self._timed_order_thread.is_alive():
            return False
        else:
            self._timed_order_thread = Thread(target=self._execute_timed_order_in_thread,
                                              args=(action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                                    duration_sec, max_order_size),
                                              daemon=True,
                                              name='Execute Timed Order Thread')
            self._is_timed_order_running = True
            self._timed_order_thread.start()
            return {'order_status' : True, 'execution_size' : 0, 'execution_message' : "Pending execution"}

    def _execute_timed_order_in_thread(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec,
                                       max_order_size):
        self.log.debug("executing timed order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange' : self.get_exchange_name(), 'action_type' : action_type, 'crypto_size': size_coin, 'price_fiat' : price_fiat,
                      'exchange_id': 0, 'order_time' : order_time, 'timed_order' : self.TIMED_ORDERS_DICT[True],
                      'status' : "Timed Order", 'crypto_type' : crypto_type,
                      'balance': self.account_balance()}
        self.log.debug("order info before execution: <%s>", order_info)
        self._db_interface.write_order_to_db(order_info)
        self._reserved_crypto_type = crypto_type
        if action_type == 'sell':
            self._reserved_crypto = size_coin
        elif action_type == 'buy':
            balance_before_order = self.account_balance()
            self._reserved_usd = price_fiat * size_coin * (1 + 0.01 * balance_before_order['fee'])
        self._timed_order_action = action_type
        self._timed_order_price_fiat = price_fiat
        action_started = False
        start_time = None
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_start_time = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = size_coin
        asset_pair = crypto_type + "-" + fiat_type
        self._timed_order_done_size = 0
        self._timed_order_elapsed_time = 0
        self._timed_order_duration_sec = duration_sec
        while self.is_timed_order_running():
            try:
                sleep_time = self.TIMED_EXECUTION_SLEEP_SEC
                self.log.debug("Timed execution status: done_size=<%f>, elapsed_time=<%f>", self._timed_order_done_size,
                               self._timed_order_elapsed_time)
                timed_order_executer = self._create_timed_order_executer(asset_pair, action_type)
                current_price_and_spread = timed_order_executer.get_current_spread_and_price()
                if not (current_price_and_spread['ask'] and current_price_and_spread['bid']):
                    self.log.warning("Missing price for <%s>", asset_pair)
                else:
                    self.log.debug("Price and spread for <%s>: <%s>", asset_pair, current_price_and_spread)
                    if not action_started:
                        # Checking that the action is within range to start execution
                        self.log.debug("Action not started yet. price_fiat=<%f>, action_type=<%s>", price_fiat, action_type)
                        if action_type == 'buy':
                            price_ratio = price_fiat / current_price_and_spread['ask']
                            if price_ratio > (1 - self.RELATIVE_RANGE_FOR_EXECUTION_START):
                                action_started = True

                        elif action_type == 'sell':
                            price_ratio = price_fiat / current_price_and_spread['bid']
                            if price_ratio < (1 + self.RELATIVE_RANGE_FOR_EXECUTION_START):
                                action_started = True

                        if action_started:
                            self.log.info("Timed order execution started")
                            start_time = time.time()
                            start_timestamp = datetime.datetime.utcnow()
                            self._timed_order_execution_start_time = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')

                    if action_started:
                        self._timed_order_elapsed_time = time.time() - start_time
                        required_execution_rate = size_coin / duration_sec
                        actual_execution_rate = required_execution_rate
                        self.log.debug("_timed_order_elapsed_time=<%d>, required_execution_rate=<%f>",
                                       self._timed_order_elapsed_time, required_execution_rate)
                        if self._timed_order_done_size != 0 and self._timed_order_elapsed_time != 0:
                            actual_execution_rate = self._timed_order_done_size / self._timed_order_elapsed_time
                            self.log.debug("actual_execution_rate=<%f>", actual_execution_rate)
                        average_spread = timed_order_executer.get_average_spread()
                        spread_ratio = 1
                        if average_spread != 0:
                            spread_ratio = current_price_and_spread['spread'] / average_spread

                        if spread_ratio <= 0:
                            self.log.warning("Invalid spread ratio: <%f>, average_spread: <%f>", spread_ratio,
                                             average_spread)
                        else:
                            execution_factor = math.exp((-1) * spread_ratio * actual_execution_rate /
                                                        required_execution_rate)
                            random_value = random.random()
                            self.log.debug("executed so far: <%f> elapsed time: <%f>, required time: <%f>, spread_ratio: "
                                           "<%f>, required size: <%f>, execution factor: <%f> random value: "
                                           "<%f> actual rate <%f>, required rate <%f>",
                                           self._timed_order_done_size, self._timed_order_elapsed_time, duration_sec,
                                           spread_ratio, size_coin, execution_factor, random_value, actual_execution_rate,
                                           required_execution_rate)

                            if execution_factor > random_value:
                                relative_order = True
                                curr_order_size = size_coin - self._timed_order_done_size

                                # Making sure that after executing the order the remaining size will be
                                # allowed by the exchange
                                if curr_order_size - max_order_size < timed_order_executer.minimum_order_size():
                                    relative_order = False

                                sent_order = timed_order_executer.get_client_for_order().send_immediate_order(
                                    action_type, curr_order_size, crypto_type, price_fiat, fiat_type, relative_order,
                                    max_order_size)
                                if sent_order is not None and sent_order['execution_size'] > 0:
                                    self._timed_order_done_size += sent_order['execution_size']
                                    sleep_time += random.uniform(self.EXECUTED_ORDER_MIN_DELAY_SEC,
                                                                 self.EXECUTED_ORDER_MAX_DELAY_SEC)

                        if action_type == 'sell':
                            self._reserved_crypto = size_coin - self._timed_order_done_size
                        elif action_type == 'buy':
                            self._reserved_usd = (size_coin - self._timed_order_done_size) * price_fiat * \
                                                             (1 + 0.01 * self.exchange_fee(crypto_type))

                if self._timed_order_done_size >= size_coin:
                    self._is_timed_order_running = False
                else:
                    time.sleep(sleep_time)
            except Exception as e:
                self.log.error("Unexpected error during timed order: %s, %s",
                               str(e), traceback.extract_tb(sys.exc_info()))
        self._reserved_crypto = 0
        self._reserved_usd = 0
        self._order_complete(True)

        self.log.info("Timed action finished")

    def send_immediate_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, relative_size,
                             max_order_size):
        sent_order = None
        execution_message = ''
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'price_fiat': price_fiat,
                      'order_time': order_time, 'timed_order': self.TIMED_ORDERS_DICT[relative_size],
                      'status': "Init", 'crypto_type': crypto_type}
        print("Immediate order:", order_info)
        try:
            execute_size_coin = size_coin
            price_and_spread = None
            if relative_size:
                price_and_spread = self._orderbook['orderbook'].get_current_spread_and_price(crypto_type + "-USD")

            if action_type == 'buy' or action_type == 'buy_limit':
                if relative_size:
                    if price_and_spread['ask'] > price_fiat:
                        self.log.info("price is too high: <%f> maximum price: <%f>",price_and_spread['ask'], price_fiat)
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['ask'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    execute_size_coin = max(execute_size_coin, self.minimum_order_size(crypto_type + "-USD"))
                    self.log.debug("size: <%f> execute_size: <%f> max_order_size: <%f>", size_coin, execute_size_coin,
                                   max_order_size)

                if execute_size_coin > 0:
                    order_info['crypto_size'] = execute_size_coin
                    self.log.info("Buying <%f> <%s> for <%f> as <%s>", execute_size_coin, crypto_type, price_fiat,
                                  action_type)
                    print("Buying <{}> <{}> for <{}> as {}".format(execute_size_coin, crypto_type, price_fiat,
                                                                   action_type))
                    if action_type == 'buy':
                        sent_order = self.buy_immediate_or_cancel(execute_size_coin, price_fiat,
                                                              self.CRYPTO_CURRENCIES_DICT[crypto_type])
                    elif action_type == 'buy_limit':
                        sent_order = self.buy_limit(execute_size_coin, price_fiat,
                                                    self.CRYPTO_CURRENCIES_DICT[crypto_type])
                    print("Sent order:", sent_order)
                    self.log.debug("sent order: <%s>", str(sent_order))
            elif action_type == 'sell' or action_type == 'sell_limit':
                print(action_type, execute_size_coin)
                if relative_size:
                    if price_and_spread['bid'] < price_fiat:
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['bid'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    execute_size_coin = max(execute_size_coin, self.minimum_order_size(crypto_type + "-USD"))
                    self.log.debug("size: <%f> execute_size: <%f>, max_order_size: <%f>", size_coin, execute_size_coin,
                                   max_order_size)

                if execute_size_coin > 0:
                    print("Selling", execute_size_coin)
                    order_info['crypto_size'] = execute_size_coin
                    self.log.info("Selling <%f> <%s> with limit of <%f>", execute_size_coin, crypto_type, price_fiat)
                    print("Selling <{}> <{}> with limit of <{}>".format(execute_size_coin, crypto_type, price_fiat))
                    if action_type == 'sell':
                        sent_order = self.sell_immediate_or_cancel(execute_size_coin, price_fiat,
                                                                   self.CRYPTO_CURRENCIES_DICT[crypto_type])
                    elif action_type == 'sell_limit':
                        print("Sell limit {} for {}".format(execute_size_coin, price_fiat))
                        sent_order = self.sell_limit(execute_size_coin, price_fiat,
                                                     self.CRYPTO_CURRENCIES_DICT[crypto_type])
                    self.log.debug("sent order: <%s>", str(sent_order))
            if execute_size_coin > 0:
                self.log.debug("Order sent in size <%f> for action <%s>", execute_size_coin, action_type)
        except Exception as e:
            order_info = None
            self.log.error("Order not sent: <%s>", str(e))
            print(e)
            sent_order = None

        order_status = False
        if sent_order is not None:
            order_id = sent_order.get("id")
            order_info['exchange_id'] = order_id
            order_info['status'] = sent_order['status']
            order_info['price_fiat'] = sent_order['executed_price_usd']
            order_status = sent_order['order_status']

        self.log.info("Order done, size is <%f>", execute_size_coin)
        print("Order info", order_info)
        if order_info is not None and execute_size_coin > 0:
            self._balance_changed = True

            order_info['balance'] = self.account_balance()
            self._db_interface.write_order_to_db(order_info)
        return {'execution_size': execute_size_coin, 'execution_message': execution_message,
                'order_status': order_status}

    def _get_relative_size(self,order_size, min_factor, max_factor):
        return random.uniform(min_factor, max_factor) * order_size

    def _get_order_size_limit(self, execute_size_coin, max_order_size):
        if max_order_size > 0 and execute_size_coin > max_order_size:
            execute_size_coin = self._get_relative_size(max_order_size, self.MAX_EXECUTION_MIN_FACTOR,
                                                        self.MAX_EXECUTION_MAX_FACTOR)
            execute_size_coin = float(min(Decimal(execute_size_coin).quantize(Decimal('1e-4')), max_order_size))
        return execute_size_coin

    def is_timed_order_running(self):
        return self._is_timed_order_running

    def get_timed_order_status(self):
        return {'timed_order_running': self.is_timed_order_running(),
                'action_type': self._timed_order_action,
                'timed_order_required_size': self._timed_order_required_size,
                'timed_order_done_size': self._timed_order_done_size,
                'timed_order_sent_time': self._timed_order_start_time,
                'timed_order_execution_start_time': self._timed_order_execution_start_time,
                'timed_order_elapsed_time': self._timed_order_elapsed_time,
                'timed_order_duration_sec': self._timed_order_duration_sec,
                'timed_order_price_fiat': self._timed_order_price_fiat}

    def cancel_timed_order(self):
        result = False
        if self._is_timed_order_running:
            self._is_timed_order_running = False
            result = True

        return result

    def get_signed_in_credentials(self):
        return {'signed_in_user': self._signed_in_user}

    def logout(self):
        self.set_credentials({})
        self._is_client_init = False
        self._last_balance = {}
        return True

    def _get_pair_price(self, crypto_type):
        asset_pair = crypto_type + "-USD"
        result = 0
        if asset_pair in self._orderbook['orderbook'].get_assets_pair():
            orderbook_price = self._orderbook['orderbook'].get_current_price(asset_pair)
            if orderbook_price['ask'] is not None and orderbook_price['bid'] is not None:
                result = (float(orderbook_price['ask']) + float(orderbook_price['bid'])) / 2

        return result

    def _get_balance_from_exchange(self):
        return {}

    def get_exchange_name(self):
        return ""

    def buy_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return {}

    def sell_immediate_or_cancel(self, execute_size_coin, price_fiat, crypto_type):
        return {}

    def order_status(self, order_id):
        return {}

    def transactions(self, transactions_limit):
        return []

    def exchange_fee(self, crypto_type):
        return 0

    def minimum_order_size(self, asset_pair):
        minimum_sizes = {'BTC-USD': 0.0006, 'BCH-USD': 0.001}
        return minimum_sizes[asset_pair]

    def is_client_initialized(self):
        return self._is_client_init

    def _order_complete(self, is_timed_order):
        if self._clients_manager:
            if is_timed_order:
                self._clients_manager.set_last_status(self.get_timed_order_status())
                print("Setting last status", self.get_timed_order_status())

    def _create_timed_order_executer(self, asset_pair, action_type):
        return TimedOrderExecuter(self, self._orderbook, asset_pair)

    def buy_limit(self, execute_size_coin, price_fiat, crypto_type):
        return {}

    def sell_limit(self, execute_size_coin, price_fiat, crypto_type):
        return {}
