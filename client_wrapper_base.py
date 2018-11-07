import datetime
import time
from threading import Thread, Lock
import random
import math
from timed_order_executer import TimedOrderExecuter
from decimal import Decimal
import logging
import traceback
import sys
from order_tracker import OrderTracker
import copy
from orderbook_base import OrderbookFee



class ClientWrapperBase:
    TIMED_EXECUTION_SLEEP_SEC = 0.5
    EXECUTED_ORDER_MIN_DELAY_SEC = 2
    EXECUTED_ORDER_MAX_DELAY_SEC = 8
    ORDER_EXECUTION_MIN_FACTOR = 0.3
    ORDER_EXECUTION_MAX_FACTOR = 0.7
    MAX_EXECUTION_MIN_FACTOR = 0.6
    MAX_EXECUTION_MAX_FACTOR = 1
    RELATIVE_RANGE_FOR_EXECUTION_START = 0.001
    TIMED_ORDERS_DICT = { True: 1, False: 0}
    CRYPTO_CURRENCIES_DICT = {'BTC': 'btc', 'BCH': 'bch'}
    LAST_ORDERS_FOR_LIMIT_AVERAGE = 5
    MAKE_ORDER_MINIMUM_SLEEP_SEC = 2
    MAKE_ORDER_MAXIMUM_SLEEP_SEC = 5
    MAKE_ORDER_CANCEL_MIN_SLEEP_SEC = 0.5
    MAKE_ORDER_CANCEL_MAX_SLEEP_SEC = 1.5
    BID_ASK_RATIO_SIZE_LIMIT = 0.2
    MINIMUM_REMAINING_SIZE = 0.0001

    def __init__(self, orderbook, db_interface, clients_manager):
        self.log = logging.getLogger('smart-trader')
        self._timed_command_listener = None
        self._timed_take_order_thread = None
        self._timed_make_order_thread = None
        self._last_balance = {}
        self._transactions = []
        self._is_timed_order_running = False
        self._timed_order_action = ''
        self._timed_order_price = 0
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
        self._client_mutex = Lock()
        self._should_have_balance = False
        self._client_credentials = client_credentials = None

    def set_credentials(self, client_credentials, cancel_order=True):
        self._client_credentials = client_credentials

    def set_balance_changed(self):
        self._balance_changed = True

    def account_balance(self, reconnect=True, extended_info=True):
        total_usd_value = 0
        if not self._is_client_init:
            self._last_balance.pop('balances', None)
            self._last_balance['total_usd_value'] = 0
        elif self._is_client_init and self._balance_changed:
            try:
                self._client_mutex.acquire()
                balances = self._get_balance_from_exchange()
                if 'error' not in balances:
                    self._last_balance['balances'] = balances
                    for balance in self._last_balance['balances']:
                        if balance == "USD":
                            total_usd_value += self._last_balance['balances'][balance]['available']
                        elif 'price' in self._last_balance['balances'][balance]:
                            self._last_balance['balances'][balance]['price'] = self._get_pair_price(balance)
                            total_usd_value += self._last_balance['balances'][balance]['price'] * \
                                               self._last_balance['balances'][balance]['available']

                    for crypto_type_key in ClientWrapperBase.CRYPTO_CURRENCIES_DICT:
                        if crypto_type_key not in self._last_balance['balances']:
                            self._last_balance['balances'][crypto_type_key] = {'amount': 0.0, 'available': 0.0,
                                                                               'price': 0.0}
                    self._last_balance['total_usd_value'] = total_usd_value
                    if total_usd_value > 0:
                        self._should_have_balance = True

                    if total_usd_value == 0 and self._should_have_balance and reconnect:
                        self.set_credentials(self._client_credentials, False)
                        return self.account_balance(False)
                    self._balance_changed = False
            except Exception as e:
                self.log.error(e)
            finally:
                self._client_mutex.release()
        self._last_balance['reserved_crypto'] = self._reserved_crypto
        self._last_balance['reserved_crypto_type'] = self._reserved_crypto_type
        self._last_balance['server_usd_reserved'] = self._reserved_usd
        self._last_balance['fees'] = self._orderbook['fees']
        result = self._last_balance
        if not extended_info:
            result = dict()
            if 'balances' not in self._last_balance:
                self._last_balance['balances'] = dict()
            for balance in self._last_balance['balances']:
                result[balance] = {'amount': self._last_balance['balances'][balance]['amount'],
                                   'available': self._last_balance['balances'][balance]['available']}
        return result

    def send_order(self, action_type, size_coin, currency_to, price, currency_from, duration_sec, max_order_size,
                   report_status, external_order_id, user_quote_price, user_id, parent_trade_order_id=-1):
        order_sent = {'order_status': False, 'execution_size': 0, 'execution_message': ''}
        if not self.is_client_initialized():
            order_sent['execution_message'] = 'Exchange client not initialized'
        else:
            order_allowed = self.can_send_order(action_type, size_coin, currency_to, price, currency_from,
                                                duration_sec)
            if not order_allowed['can_send_order']:
                order_sent['execution_message'] = order_allowed['reason']
                self.log.warning("Order not allowed: <%s>", order_allowed['reason'])
            else:
                if duration_sec == 0:
                    order_sent = self.send_immediate_order(action_type, size_coin, currency_to, price, currency_from,
                                                           False, 0, False, -1, external_order_id, user_quote_price,
                                                           user_id)
                    self._order_complete(False, True)
                else:
                    actions_dict = {'buy': 'buy', 'sell': 'sell', 'timed_sell': 'sell', 'timed_buy': 'buy', 'sell_limit': 'sell_limit',
                                    'buy_limit': 'buy_limit', "buy_market": "buy_market", "sell_market": "sell_market"}
                    self.log.info("Time order, action: <%s>, size_coin: <%f>, currency_to: <%s>, price: <%f> "
                                  "currency_from: <%s>, duration_sec: <%f>, max_order_size: <%f>",
                                  actions_dict[action_type], size_coin, currency_to, price, currency_from,
                                  duration_sec, max_order_size)
                    if not self.is_timed_order_running():
                        if action_type == 'sell_limit' or action_type == 'buy_limit':
                            order_sent = self.execute_timed_make_order(actions_dict[action_type], size_coin,
                                                                       currency_from, currency_to, price,
                                                                       duration_sec, max_order_size, report_status,
                                                                       external_order_id, user_quote_price, user_id,
                                                                       parent_trade_order_id)
                        else:
                            order_sent = self.execute_timed_take_order(actions_dict[action_type], size_coin,
                                                                       currency_from, currency_to, price,
                                                                       duration_sec, max_order_size, external_order_id,
                                                                       user_quote_price, user_id)
                    else:
                        self.log.warning("Timed order already running, ignoring new timed order")
        return order_sent

    def can_send_order(self, action_type, size_coin, crypto_type, price, fiat_type, duration_sec):
        result, refuse_reason = ClientWrapperBase.verify_order_params(size_coin, price, duration_sec)
        if result:
            action_types_dict = {'sell': 'sell', 'timed_sell': 'sell', 'sell_limit': 'sell',
                                 'buy': 'buy', 'timed_buy': 'buy', 'buy_limit': 'buy',
                                 'buy_market':'buy_market', 'sell_market':'sell_market' }
            fee_type_dict = {'buy': 'take', 'buy_limit': 'make', 'timed_buy': 'take'}
            if action_type not in action_types_dict:
                result = False
                refuse_reason = 'Invalid action: {}, valid actions are: {}'.format(action_type,
                                                                                   action_types_dict.values())
            else:
                check_action_type = action_types_dict[action_type]
                if price:
                    balance_before_order = self.account_balance()
                    self.log.debug("balance_before_order <%s>", str(balance_before_order))
                    if result and check_action_type == 'sell' and size_coin > \
                            balance_before_order['balances'][crypto_type]['available']:
                        refuse_reason = "Available balance " + \
                                        str(balance_before_order['balances'][crypto_type]['available']) + \
                                        crypto_type + " is less than required size " + str(size_coin) + crypto_type
                        result = False
                    elif result and check_action_type == 'buy' and (
                            price * size_coin * (1 + 0.01 * self._orderbook['orderbook'].get_fees(
                            )[fee_type_dict[action_type]])) > \
                            float(balance_before_order['balances'][fiat_type]['available']):
                        refuse_reason = "Available balance " + str(
                            balance_before_order['balances'][fiat_type]['available']) + \
                            "USD is less than required balance " + \
                            str(price * size_coin * (1 + 0.01 * self._orderbook['orderbook'].get_fees(
                            )[fee_type_dict[action_type]]))
                        result = False
        return {'can_send_order': result, 'reason': refuse_reason}

    @staticmethod
    def verify_order_params(size_coin, price, duration_sec):
        result = True
        refuse_reason = ''
        try:
            refuse_reason = "Invalid size"
            if float(size_coin) <= 0:
                result = False

            if result and price:
                refuse_reason = "Invalid price"
                price = float(price)
                if float(price) < 0:
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

    def execute_timed_take_order(self, action_type, size_coin, currency_from, currency_to, price, duration_sec,
                                 max_order_size, external_order_id, user_quote_price, user_id):
        if self._timed_take_order_thread is not None and self._timed_take_order_thread.is_alive():
            return False
        else:
            self._timed_take_order_thread = Thread(target=self._execute_timed_take_order_in_thread,
                                                   args=(action_type, size_coin, currency_from, currency_to, price,
                                                         duration_sec, max_order_size, external_order_id,
                                                         user_quote_price, user_id),
                                                   daemon=True,
                                                   name='Execute Timed Take Order Thread')
            self._is_timed_order_running = True
            self._timed_take_order_thread.start()
            return {'order_status': True, 'execution_size': 0, 'execution_message': "Pending execution"}

    def _execute_timed_take_order_in_thread(self, action_type, size_coin, currency_from, currency_to, price,
                                            duration_sec, max_order_size, external_order_id, user_quote_price, user_id):
        self.log.debug("executing timed take order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'size': size_coin,
                      'price': price, 'exchange_id': 0, 'order_time': order_time,
                      'timed_order': self.TIMED_ORDERS_DICT[True], 'status': "Timed Take Order",
                      'currency_from': currency_from,
                      'currency_to': currency_to,
                      'balance': self.account_balance(),
                      'external_order_id': external_order_id,
                      'user_quote_price': user_quote_price, 'user_id': user_id}
        self.log.debug("order info before execution: <%s>", order_info)
        parent_trade_id = self._db_interface.write_order_to_db(order_info)
        self._reserved_crypto_type = currency_to
        if action_type == 'sell':
            self._reserved_crypto = size_coin
        elif action_type == 'buy':
            self._reserved_usd = price * size_coin * (1 + 0.01 * self._orderbook['orderbook'].get_fees()['take'])
        self._timed_order_action = action_type
        self._timed_order_price = price
        action_started = False
        start_time = None
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = size_coin
        asset_pair = currency_to + "-" + currency_from
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
                        self.log.debug("Action not started yet. price=<%f>, action_type=<%s>", price, action_type)
                        if action_type == 'buy':
                            price_ratio = price / current_price_and_spread['ask']['price']
                            if price_ratio > (1 - self.RELATIVE_RANGE_FOR_EXECUTION_START):
                                action_started = True

                        elif action_type == 'sell':
                            price_ratio = price / current_price_and_spread['bid']['price']
                            if price_ratio < (1 + self.RELATIVE_RANGE_FOR_EXECUTION_START):
                                action_started = True

                        if action_started:
                            self.log.info("Timed order execution started")
                            start_time = time.time()
                            start_timestamp = datetime.datetime.utcnow()
                            self._timed_order_execution_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')

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
                                           "<%f> actual rate <%f>, required rate <%f>, parent order id <%d>",
                                           self._timed_order_done_size, self._timed_order_elapsed_time, duration_sec,
                                           spread_ratio, size_coin, execution_factor, random_value, actual_execution_rate,
                                           required_execution_rate, parent_trade_id)

                            if execution_factor > random_value:
                                relative_order = True
                                curr_order_size = size_coin - self._timed_order_done_size

                                # Making sure that after executing the order the remaining size will be
                                # allowed by the exchange
                                if curr_order_size - max_order_size < timed_order_executer.minimum_order_size():
                                    relative_order = False

                                sent_order = timed_order_executer.get_client_for_order().send_immediate_order(
                                    action_type, curr_order_size, currency_to, price, currency_from, relative_order,
                                    max_order_size, True, parent_trade_id, external_order_id, user_quote_price, user_id)
                                if sent_order is not None and sent_order['execution_size'] > 0:
                                    self._timed_order_done_size += sent_order['execution_size']
                                    sleep_time += random.uniform(self.EXECUTED_ORDER_MIN_DELAY_SEC,
                                                                 self.EXECUTED_ORDER_MAX_DELAY_SEC)

                        if action_type == 'sell':
                            self._reserved_crypto = size_coin - self._timed_order_done_size
                        elif action_type == 'buy':
                            self._reserved_usd = (size_coin - self._timed_order_done_size) * price * \
                                                             (1 + 0.01 * timed_order_executer.get_client_for_order(
                                                                 ).exchange_fee(currency_to))

                if self._timed_order_done_size >= size_coin:
                    self._is_timed_order_running = False
                else:
                    time.sleep(sleep_time)
            except Exception as e:
                self.log.error("Unexpected error during timed order: %s, %s",
                               str(e), traceback.extract_tb(sys.exc_info()))
        self._reserved_crypto = 0
        self._reserved_usd = 0
        self._order_complete(True, True)

        self.log.info("Timed action finished")

    def send_immediate_order(self, action_type, size_coin, currency_to, price, currency_from, relative_size,
                             max_order_size, is_timed_order, parent_trade_order_id, external_order_id, user_quote_price,
                             user_id):
        self.log.info("send_immediate: my type: <%s>, action_type: <%s>, size_coin: <%f>, coin_to: <%s>, price: <%f>, "
                      "parent_trade_order_id: <%s>", type(self), action_type, size_coin, currency_to, price,
                      parent_trade_order_id)
        sent_order = None
        execution_message = ''
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'price': price,
                      'order_time': order_time, 'timed_order': self.TIMED_ORDERS_DICT[is_timed_order],
                      'status': "Init", 'currency_to': currency_to, 'parent_trade_order_id': parent_trade_order_id,
                      'external_order_id': external_order_id, 'user_quote_price': user_quote_price, 'user_id': user_id,
                      'currency_from': currency_from}
        self.log.debug("Immediate order: <%s>", order_info)
        #try:
        execute_size_coin = size_coin
        price_and_spread = None
        # if relative_size:
        price_and_spread = self._orderbook['orderbook'].get_current_spread_and_price(currency_to + "-" + currency_from)
        try:
            self._client_mutex.acquire()
            if action_type == 'buy' or action_type == 'buy_limit' or action_type == 'buy_market':
                if relative_size:
                    if price_and_spread['ask']['price'] > price:
                        self.log.info("price is too high: <%f> maximum price: <%f>", price_and_spread['ask'], price)
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['ask']['price'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    execute_size_coin = max(execute_size_coin, self.minimum_order_size(
                        currency_to + "-" + currency_from))
                    self.log.debug("size: <%f> execute_size: <%f> max_order_size: <%f>", size_coin, execute_size_coin,
                                   max_order_size)
                execute_size_coin = float(Decimal(execute_size_coin).quantize(Decimal('1e-4')))
                if execute_size_coin > 0:
                    order_info['size'] = execute_size_coin
                    self.log.info("<%s> <%f> <%s>-<%s> with limit of <%f>", action_type, execute_size_coin, currency_to,
                                  currency_from, price)
                    if action_type == 'buy':
                        sent_order = self.buy_immediate_or_cancel(execute_size_coin, price, currency_from, currency_to)
                    elif action_type == 'buy_limit':
                        sent_order = self.buy_limit(execute_size_coin, price, currency_from, currency_to)
                    elif action_type == 'buy_market':
                        sent_order = self.buy_market(execute_size_coin, currency_from, currency_to)
                    self.log.debug("sent order: <%s>", str(sent_order))
            elif action_type == 'sell' or action_type == 'sell_limit' or action_type == 'sell_market':
                if relative_size:
                    if price_and_spread['bid']['price'] < price:
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['bid']['price'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    execute_size_coin = max(execute_size_coin,
                                            self.minimum_order_size(currency_to + "-" + currency_from))
                    self.log.debug("size: <%f> execute_size: <%f>, max_order_size: <%f>", size_coin, execute_size_coin,
                                   max_order_size)
                execute_size_coin = float(Decimal(execute_size_coin).quantize(Decimal('1e-4')))
                if execute_size_coin > 0:
                    order_info['size'] = execute_size_coin
                    self.log.info("<%s> <%f> <%s>-<%s> with limit of <%f>", action_type, execute_size_coin, currency_to,
                                  currency_from, price)
                    if action_type == 'sell':
                        sent_order = self.sell_immediate_or_cancel(execute_size_coin, price, currency_from, currency_to)
                    elif action_type == 'sell_limit':
                        sent_order = self.sell_limit(execute_size_coin, price, currency_from, currency_to)
                    elif action_type == 'sell_market':
                        sent_order = self.sell_market(execute_size_coin, currency_from, currency_to)
                    self.log.debug("sent order: <%s>", str(sent_order))
            if execute_size_coin > 0:
                self.log.debug("Order sent in size <%f> for action <%s>", execute_size_coin, action_type)
        except Exception as e:
            order_info = None
            self.log.error("Order not sent: <%s>", str(e))
            sent_order = None
            execute_size_coin = 0
        finally:
            self._client_mutex.release()
        order_status = False
        done_size = 0
        if sent_order is not None:
            order_id = sent_order.get("id")
            order_info['exchange_id'] = order_id
            self.log.debug("Sent order: <%s>", sent_order)
            order_info['status'] = sent_order['status']
            order_info['price'] = sent_order['executed_price_usd']
            order_status = sent_order['order_status']

            if 'execution_message' in sent_order:
                execution_message = sent_order['execution_message']

            if order_info['status'] == 'Finished':
                done_size = execute_size_coin
                self.log.info("Order done, size is <%f>", done_size)
        
        trade_order_id = -1
        if order_info is not None:
            if done_size > 0:
                self._balance_changed = True
                order_info['balance'] = self.account_balance()
            order_info["ask"] = 0
            order_info["bid"] = 0
            if price_and_spread and 'ask' in price_and_spread and 'price' in price_and_spread['ask']:
                order_info["ask"] = price_and_spread["ask"]["price"]
            if price_and_spread and 'bid' in price_and_spread and 'price' in price_and_spread['bid']:
                order_info["bid"] = price_and_spread["bid"]["price"]
            trade_order_id = self._db_interface.write_order_to_db(order_info)

        return {'execution_size': done_size, 'execution_message': execution_message,
                'order_status': order_status, 'trade_order_id': trade_order_id}

    @staticmethod
    def _get_relative_size(order_size, min_factor, max_factor):
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
                'timed_order_price_fiat': self._timed_order_price}

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

    def _get_pair_price(self, currency_from, currency_to):
        asset_pair = currency_to + "-" + currency_from
        result = 0
        if asset_pair in self._orderbook['orderbook'].get_asset_pairs():
            orderbook_price = self._orderbook['orderbook'].get_current_price(asset_pair, OrderbookFee.NO_FEE)
            if orderbook_price['ask'] is not None and orderbook_price['bid'] is not None:
                result = (float(orderbook_price['ask']['price']) + float(orderbook_price['bid']['price'])) / 2

        return result

    def execute_timed_make_order(self, action_type, size_coin, currency_from, currency_to, price, duration_sec,
                                 max_order_size, report_status, external_order_id, user_quote_price, user_id,
                                 parent_trade_order_id):
        if self._timed_make_order_thread is not None and self._timed_make_order_thread.is_alive():
            return False
        else:
            self._timed_make_order_thread = Thread(target=self._execute_timed_make_order_in_thread,
                                                   args=(action_type, float(size_coin), currency_from, currency_to,
                                                         float(price), int(duration_sec), float(max_order_size), 0.2, 0,
                                                         bool(report_status), external_order_id, user_quote_price,
                                                         user_id, parent_trade_order_id),
                                                   daemon=True,
                                                   name='Execute Timed Make Order Thread')
            self._is_timed_order_running = True
            self._timed_make_order_thread.start()
            return {'order_status': True, 'execution_size': 0, 'execution_message': "Pending execution"}

    def _execute_timed_make_order_in_thread(self, action_type, size_coin, currency_from, currency_to, price,
                                            duration_sec, max_order_size, max_relative_spread_factor,
                                            relative_to_best_order_ratio, report_status, external_order_id,
                                            user_quote_price, user_id, parent_trade_order_id):
        self.log.debug("executing timed make order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'size': size_coin,
                      'price': price, 'exchange_id': 0, 'order_time': order_time,
                      'timed_order': self.TIMED_ORDERS_DICT[True], 'status': "Make Order",
                      'currency_from': currency_from, 'currency_to': currency_to,
                      'balance': self.account_balance(), 'external_order_id': external_order_id,
                      'user_quote_price': user_quote_price, 'user_id': user_id}
        self.log.info("order info before execution: <%s>", order_info)
        db_trade_order_id = self._db_interface.write_order_to_db(order_info)
        if parent_trade_order_id == -1:
            parent_trade_order_id = db_trade_order_id
        order_info['parent_trade_order_id'] = parent_trade_order_id
        self._reserved_crypto_type = currency_to
        """if action_type == 'sell':
            self._reserved_crypto = size_coin
        elif action_type == 'buy':
            balance_before_order = self.account_balance()
            self._reserved_usd = price * size_coin * (1 + 0.01 * balance_before_order['fee'])"""
        self._timed_order_action = action_type
        self._timed_order_price = price
        action_started = False
        start_time = None
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = size_coin
        asset_pair = currency_to + "-" + currency_from
        self._timed_order_done_size = 0
        self._timed_order_elapsed_time = 0
        self._timed_order_duration_sec = duration_sec
        active_order = None
        active_order_tracker = None
        timed_order_start_time = time.time()
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_execution_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        prev_order_price = 0
        tracked_order = None
        while self.is_timed_order_running():
            self._timed_order_elapsed_time = time.time() - timed_order_start_time
            if active_order_tracker and tracked_order['status'] != 'Finished':
                active_order_tracker.update_order_from_exchange()
            execute_order_on_current_market = True
            current_price_and_spread = self._orderbook['orderbook'].get_current_spread_and_price(
                asset_pair, OrderbookFee.MAKER_FEE)
            new_order_price = 0
            price_changed = False
            if current_price_and_spread is None:
                self.log.warning("Missing price for making orders: action: <%s>, size: <%f>, price: <%f>",
                                 action_type, size_coin, price)
                execute_order_on_current_market = False
            else:
                spread_difference = current_price_and_spread['spread'] * \
                                    random.uniform(0.5 * max_relative_spread_factor, max_relative_spread_factor)
                if action_type == 'buy_limit':
                    new_order_price = current_price_and_spread['bid']['price'] + spread_difference
                elif action_type == 'sell_limit':
                    new_order_price = current_price_and_spread['ask']['price'] - spread_difference
                new_order_price = float(Decimal(new_order_price).quantize(Decimal(self.exchange_accuracy())))
                if new_order_price != prev_order_price:
                    price_changed = True

                fee = self._orderbook['orderbook'].get_fees()['make']
                if (action_type == 'buy_limit' and new_order_price > (price * (1 - 0.01 * fee))) or \
                        (action_type == 'sell_limit' and new_order_price < (price * (1 + 0.01 * fee))):
                    self.log.debug("Order for <%s> is out of market price: <%s>, price: <%s>, order price: <%f>",
                                   action_type, order_info, current_price_and_spread, price)
                    execute_order_on_current_market = False

            additional_sleep_time_for_cancel = 0
            if active_order is not None and (price_changed or not execute_order_on_current_market):
                self.log.debug("Done size: <%f> cancelling order if not done: <%s>", self._timed_order_done_size,
                               str(active_order))
                cancel_status = None
                try:
                    self._client_mutex.acquire()
                    cancel_status = self._cancel_order(active_order['id'])
                    self.log.debug("Cancel status: <%s>", str(cancel_status))
                finally:
                    self._client_mutex.release()
                tracked_order['order_time'] = datetime.datetime.utcnow()
                already_written_to_db = False
                if cancel_status:
                    self.log.debug("Cancelling order <%s>", str(active_order))
                    tracked_order['status'] = "Cancelled"
                    additional_sleep_time_for_cancel = random.uniform(ClientWrapperBase.MAKE_ORDER_CANCEL_MIN_SLEEP_SEC,
                                                                      ClientWrapperBase.MAKE_ORDER_CANCEL_MAX_SLEEP_SEC)
                    time.sleep(additional_sleep_time_for_cancel)

                # Cancelling failed so the order was done
                elif active_order and active_order['executed_size'] != active_order['required_size']:
                    self.log.debug("Getting information for order <%s> from exchange transactions", active_order['id'])
                    if active_order['executed_size'] > 0:
                        # Reducing the current size because it will be recalculated from the transactions
                        self.add_order_executed_size(-1 * active_order['executed_size'], None, None, None)
                    active_order_tracker.update_order_from_transactions()
                    self.log.debug("Tracked order after update from transactions: <%s>", str(tracked_order))
                    if active_order['executed_size'] == 0:
                        tracked_order['status'] = "Cancelled"
                    else:
                        tracked_order['status'] = "Make Order Executed"
                elif active_order and active_order['executed_size'] == active_order['required_size']:
                    self.log.debug("Not writing order <%s> to db because it's complete", active_order['id'])
                    already_written_to_db = True

                if not already_written_to_db:
                    self.log.debug("Writing order to DB: <%s>", str(tracked_order))
                    trade_order_id = self._db_interface.write_order_to_db(tracked_order)
                    tracked_order['trade_order_id'] = trade_order_id
                active_order_tracker.unregister_order()
                active_order = None
                active_order_tracker = None
            elif execute_order_on_current_market:
                self.log.debug("Current order <%s> still valid on current price and spread <%s>", str(tracked_order),
                               str(current_price_and_spread))

            if execute_order_on_current_market and price_changed:
                new_order_size = size_coin - self._timed_order_done_size
                price_size_ratio = random.uniform(ClientWrapperBase.BID_ASK_RATIO_SIZE_LIMIT,
                                                  1 - ClientWrapperBase.BID_ASK_RATIO_SIZE_LIMIT)
                size_from_price = price_size_ratio * current_price_and_spread['ask']['size'] + \
                                  (1 - price_size_ratio) * current_price_and_spread['bid']['size']
                size_from_price = float(Decimal(size_from_price).quantize(Decimal('1e-4')))
                new_order_size = min(new_order_size, size_from_price,
                                     random.uniform(ClientWrapperBase.MAX_EXECUTION_MIN_FACTOR * max_order_size,
                                                    max_order_size))
                if size_coin - self._timed_order_done_size - new_order_size < \
                    self.minimum_order_size(currency_to + "-" + currency_from):
                    new_order_size = size_coin - self._timed_order_done_size
                new_order_size = float(Decimal(new_order_size).quantize(Decimal('1e-4')))
                if new_order_size > 0:
                    self.log.debug("New order size: <%f>,  new order price: <%f>, price and spread: <%s>, " \
                                   "spread difference: <%f>, remaining size <%f>", new_order_size, new_order_price,
                                   current_price_and_spread, spread_difference, size_coin - self._timed_order_done_size)
                    if action_type == 'buy_limit':
                        active_order = self.buy_limit(new_order_size, new_order_price, currency_from, currency_to)
                    elif action_type == 'sell_limit':
                        active_order = self.sell_limit(new_order_size, new_order_price, currency_from, currency_to)
                    active_order['required_size'] = new_order_size
                    active_order['executed_size'] = 0
                    self.log.debug("New order: <%s>", str(active_order))
                    prev_order_price = new_order_price

                    if active_order and (active_order['status'] == 'Open' or active_order['status'] == 'Finished'):
                        tracked_order = copy.deepcopy(order_info)
                        tracked_order['exchange_id'] = active_order['id']
                        tracked_order['size'] = new_order_size
                        tracked_order['balance'] = self.account_balance()
                        tracked_order['price'] = new_order_price
                        tracked_order['order_time'] = datetime.datetime.utcnow()
                        if active_order['status'] == 'Open':
                            tracked_order['status'] = "Make Order Sent"
                            tracked_order['executed_size'] = 0
                        else:
                            tracked_order['status'] = "Finished"
                            tracked_order['executed_size'] = new_order_size
                            self.add_order_executed_size(new_order_size, None, None, None)
                        self.log.debug("Setting price")
                        tracked_order['ask'] = current_price_and_spread['ask']['price']
                        tracked_order['bid'] = current_price_and_spread['bid']['price']
                        self.log.debug("Make order info: <%s>", str(tracked_order))
                        trade_order_id = self._db_interface.write_order_to_db(tracked_order)
                        tracked_order['trade_order_id'] = trade_order_id
                        active_order_tracker = self.create_order_tracker(active_order, self._orderbook, tracked_order,
                                                                         currency_from, currency_to)
                    elif active_order and active_order['status'] == 'Error':
                        active_order = None

            if self._timed_order_done_size >= size_coin - ClientWrapperBase.MINIMUM_REMAINING_SIZE:
                self._is_timed_order_running = False
            else:
                time.sleep(random.uniform(
                    ClientWrapperBase.MAKE_ORDER_MINIMUM_SLEEP_SEC, ClientWrapperBase.MAKE_ORDER_MAXIMUM_SLEEP_SEC) + \
                           additional_sleep_time_for_cancel)

        if active_order:
            self.log.info("Done size: <%f>, Cancelling order: <%s>", self._timed_order_done_size, str(active_order))
            try:
                self._client_mutex.acquire()
                self._cancel_order(active_order['id'])
            finally:
                self._client_mutex.release()
            active_order_tracker.unregister_order()

        self._order_complete(True, report_status)

    def _get_balance_from_exchange(self):
        return {}

    def get_exchange_name(self):
        return ""

    def sell_market(self, execute_size_coin, currency_from, currency_to):
        return {}

    def buy_market(self, execute_size_coin, currency_from, currency_to):
        return {}

    def buy_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return {}

    def sell_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return {}

    def order_status(self, order_id):
        return {}

    def transactions(self, transactions_limit):
        return []

    def minimum_order_size(self, asset_pair):
        minimum_sizes = {'BTC-USD': 0.002, 'BCH-USD': 0.02, 'BTC-EUR': 0.002, 'BCH-EUR': 0.02}
        result = 0
        if asset_pair in minimum_sizes:
            result = minimum_sizes[asset_pair]
        return result

    def is_client_initialized(self):
        return self._is_client_init

    def _order_complete(self, is_timed_order, report_status):
        if self._clients_manager:
            if is_timed_order and report_status:
                self._clients_manager.set_last_status(self.get_timed_order_status())
                self.log.debug("Setting last status <%s>", str(self.get_timed_order_status()))

    def _create_timed_order_executer(self, asset_pair, action_type):
        return TimedOrderExecuter(self, self._orderbook, asset_pair)

    def buy_limit(self, execute_size_coin, price, currency_from, currency_to):
        return {}

    def sell_limit(self, execute_size_coin, price, currency_from, currency_to):
        return {}

    def _cancel_order(self, order_id):
        return None

    def _cancel_active_limit_order(self):
        pass

    def add_order_executed_size(self, executed_size, price, order_info, timestamp):
        executed_size = float(Decimal(executed_size).quantize(Decimal('1e-4')))
        self.log.debug("Executed size: <%f>", executed_size)
        self._timed_order_done_size += executed_size
        self._balance_changed = True
        if order_info and price and timestamp:
            order_info['size'] = executed_size
            order_info['balance'] = self.account_balance()
            order_info['price'] = price
            order_info['order_time'] = datetime.datetime.utcnow()
            order_info['status'] = "Make Order Executed"
            self.log.debug("Make order info when updating from orders tracker: <%s>", str(order_info))
            if not ('updated_from_transactions' in order_info and order_info['updated_from_transactions']):
                self._db_interface.write_order_to_db(order_info)
        else:
            self.log.debug("No order info")
        if self._timed_command_listener:
            self._timed_command_listener.add_order_executed_size(executed_size, None, None, None)

    def set_order_executed_size(self, executed_size):
        executed_size = float(Decimal(executed_size).quantize(Decimal('1e-4')))
        added_size = executed_size - self._timed_order_done_size
        self._timed_order_done_size = executed_size
        self._balance_changed = True
        if self._timed_command_listener:
            self._timed_command_listener.add_order_executed_size(added_size, None, None, None)

    def create_order_tracker(self, order, orderbook, order_info, currency_from, currency_to):
        order['id'] = int(order['id'])
        return OrderTracker(order, orderbook, self, order_info, currency_from, currency_to)

    def exchange_accuracy(self):
        return '1e-2'

    def set_timed_command_listener(self, listener):
        self._timed_command_listener = listener

    def get_orderbook_price(self, asset_pair, include_fee=OrderbookFee.NO_FEE):
        orderbook_price = None
        if asset_pair in self._orderbook['orderbook'].get_asset_pairs():
            orderbook_price = self._orderbook['orderbook'].get_current_price(asset_pair, include_fee)
        return orderbook_price

    def join_timed_make_thread(self):
        if self._timed_make_order_thread and self._timed_make_order_thread.is_alive():
            self._timed_make_order_thread.join()
