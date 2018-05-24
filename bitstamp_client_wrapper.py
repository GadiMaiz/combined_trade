import bitstamp.client
import datetime
import time
from threading import Thread
from requests.exceptions import HTTPError
import random
import math
import sqlite3
from decimal import Decimal
import logging

class BitstampClientWrapper:
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

    def __init__(self, bitstamp_credentials, bitstamp_orderbook, db_file):
        self.log = logging.getLogger(__name__)
        self._timed_order_thread = None
        self._last_balance = {}
        self._transactions = []
        self._bitstamp_client = None
        self._is_timed_order_running = False
        self._timed_order_action = ''
        self._timed_order_price_fiat = 0
        self._timed_order_start_time = ''
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = 0
        self._timed_order_done_size = 0
        self._orderbook = bitstamp_orderbook
        self._db_file = db_file
        self._balance_changed = {}
        self._reserved_balances = {'BTC': 0, 'BCH': 0, 'USD': 0}
        self._timed_order_elapsed_time = 0
        self._timed_order_duration_sec = 0
        self._signed_in_user = ""
        self.set_client_credentails(bitstamp_credentials)

    def create_db_connection(self, db_file):
        """ create a database connection to a SQLite database """
        try:
            conn = sqlite3.connect(db_file)
        except sqlite3.Error as e:
            self.log.error("db connection error",e)
            conn = None

        return conn

    def account_balance(self, crypto_type):
        if self._bitstamp_client is None:
            self._balance_changed[crypto_type] = False
            self._last_balance[crypto_type] = {}
            self._last_balance[crypto_type]['reserved_crypto'] = 0
            self._last_balance[crypto_type]['server_usd_reserved'] = 0
        elif crypto_type not in self._balance_changed or self._balance_changed[crypto_type]:
            try:
                self._last_balance[crypto_type] = self._bitstamp_client.account_balance(crypto_type)
                self._balance_changed[crypto_type] = False
            except Exception as e:
                self.log.error(e)
        self._last_balance[crypto_type]['reserved_crypto'] = self._reserved_balances[crypto_type]
        self._last_balance[crypto_type]['server_usd_reserved'] = self._reserved_balances['USD']
        return self._last_balance[crypto_type]

    def transactions(self, transactions_limit):
        transactions = []
        try:
            if self._bitstamp_client is not None:
                self._transactions = self._bitstamp_client.user_transactions()
                transactions = self._transactions
                if transactions_limit != 0 and len(transactions) > transactions_limit:
                    transactions = transactions[0:transactions_limit]
        except:
            transactions = []
        return transactions

    def SendOrder(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        order_sent = {'order_status' : False, 'execution_size' : 0, 'execution_message' : ''}
        if self._bitstamp_client is None:
            order_sent['execution_message'] = 'Bitstamp client not initialized'
        else:
            order_allowed = self.can_send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec)
            if not order_allowed['can_send_order']:
                order_sent['execution_message'] = order_allowed['reason']
                print(order_allowed['reason'])
            else:
                if duration_sec == 0:
                    order_sent = self.send_immediate_order(action_type, size_coin, crypto_type, price_fiat, fiat_type, False, 0)
                else:
                    actions_dict = {'timed_sell': 'sell', 'timed_buy': 'buy'}
                    print ("Timed sell")
                    if not self.IsTimedOrderRunning():
                        order_sent = self.ExecuteTimedOrder(actions_dict[action_type], size_coin, crypto_type, price_fiat, fiat_type,
                                                            duration_sec, max_order_size)
        return order_sent

    def can_send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec):
        result = True
        refuse_reason = ''
        balance_before_order = self.account_balance(crypto_type)
        crypto_type_dict = {'BTC': 'btc_available', 'BCH': 'bch_available'}
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

        if result and action_type == 'sell' and size_coin > float(balance_before_order[crypto_type_dict[crypto_type]]):
            refuse_reason = "Available balance " + str(balance_before_order[crypto_type_dict[crypto_type]]) + \
                            crypto_type + " is less than required size " + str(size_coin) + crypto_type
            result = False
        elif result and action_type == 'buy' and (price_fiat * size_coin * (1 + 0.01 * balance_before_order['fee'])) > \
                float(balance_before_order['usd_available']):
            refuse_reason = "Available balance " + str(balance_before_order['usd_available']) + \
                            "USD is less than required balance " + str(price_fiat * size_coin * (1 + 0.01 * balance_before_order['fee']))
            result = False

        return {'can_send_order' : result, 'reason' : refuse_reason}


    def ExecuteTimedOrder(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        if self._timed_order_thread is not None and self._timed_order_thread.is_alive():
            return False
        else:
            self._timed_order_thread = Thread(target=self._execute_timed_order,
                                              args=(action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                                    duration_sec, max_order_size),
                                              daemon=True,
                                              name='Execute Timed Order Thread')
            self._is_timed_order_running = True
            self._timed_order_thread.start()
            return {'order_status' : True, 'execution_size' : 0, 'execution_message' : "Pending execution"}

    def _execute_timed_order(self, action_type,size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        print("executing timed order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange' : 'Bitstamp', 'action_type' : action_type, 'crypto_size': size_coin, 'price_fiat' : price_fiat,
                      'exchange_id': 0, 'order_time' : order_time, 'timed_order' : self.TIMED_ORDERS_DICT[True],
                      'status' : "Timed Order", 'crypto_type' : crypto_type,
                      'balance': self._get_available_account_balance(crypto_type)}
        self.write_order_to_db(order_info)
        reserved_type = ''
        if action_type == 'sell':
            self._reserved_balances[crypto_type] = size_coin
            reserved_type = crypto_type
        elif action_type == 'buy':
            balance_before_order = self.account_balance(crypto_type)
            self._reserved_balances['USD'] = price_fiat * size_coin * (1 + 0.01 * balance_before_order['fee'])
            reserved_type = 'USD'
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
        while self.IsTimedOrderRunning():
            sleep_time = self.TIMED_EXECUTION_SLEEP_SEC
            current_price_and_spread = self._orderbook['orderbook'].get_current_spread_and_price(asset_pair)
            if not action_started:
                # Checking that the action is within range to start execution
                if action_type == 'buy':
                    price_ratio = price_fiat / current_price_and_spread['ask']
                    if price_ratio > (1 - self.RELATIVE_RANGE_FOR_EXECUTION_START):
                        action_started = True

                elif action_type == 'sell':
                    price_ratio = price_fiat / current_price_and_spread['bid']
                    if price_ratio < (1 + self.RELATIVE_RANGE_FOR_EXECUTION_START):
                        action_started = True

                if action_started:
                    print("Timed order execution started")
                    start_time = time.time()
                    start_timestamp = datetime.datetime.utcnow()
                    self._timed_order_execution_start_time = start_timestamp.strftime('%Y-%m-%d %H:%M:%S')

            if action_started:
                self._timed_order_elapsed_time = time.time() - start_time
                required_execution_rate = size_coin / duration_sec
                actual_execution_rate = required_execution_rate
                if self._timed_order_done_size != 0 and self._timed_order_elapsed_time != 0:
                    actual_execution_rate = self._timed_order_done_size / self._timed_order_elapsed_time
                average_spread = self._orderbook['orderbook'].get_average_spread(asset_pair)
                spread_ratio = 0
                if average_spread != 0:
                    spread_ratio = current_price_and_spread['spread'] / self._orderbook['orderbook'].get_average_spread(asset_pair)
                if spread_ratio <= 0:
                    print("Invalid spread ratio:", spread_ratio, "average spread:", average_spread)
                else:
                    execution_factor = math.exp((-1) * spread_ratio * actual_execution_rate / required_execution_rate)
                    random_value = random.random()
                    print ("executed so far:", self._timed_order_done_size,"elapsed time:", self._timed_order_elapsed_time, "required time:", duration_sec, "spread ratio", spread_ratio,
                           "required size", size_coin, "execution factor: ", execution_factor, "random value: ", random_value, "actual rate", actual_execution_rate,
                           "required rate", required_execution_rate)

                    if execution_factor > random_value:
                        sent_order = self.send_immediate_order(action_type, size_coin - self._timed_order_done_size, crypto_type, price_fiat, fiat_type, True, max_order_size)
                        if sent_order is not None and sent_order['execution_size'] > 0:
                            self._timed_order_done_size += sent_order['execution_size']
                            sleep_time += random.uniform(self.EXECUTED_ORDER_MIN_DELAY_SEC, self.EXECUTED_ORDER_MAX_DELAY_SEC)

                if action_type == 'sell':
                    self._reserved_balances[crypto_type] = size_coin - self._timed_order_done_size
                elif action_type == 'buy':
                    self._reserved_balances['USD'] = (size_coin - self._timed_order_done_size) * price_fiat * (1 + 0.01 * self._last_balance[crypto_type]['fee'])

            if self._timed_order_done_size >= size_coin:
                self._is_timed_order_running = False
            else:
                time.sleep(sleep_time)
        self._reserved_balances[reserved_type] = 0

        print ("Timed sell finished")

    def send_immediate_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, relative_size, max_order_size):
        sent_order = None
        execution_message = ''
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange' : 'Bitstamp', 'action_type' : action_type, 'price_fiat' : price_fiat,
                      'order_time' : order_time, 'timed_order' : self.TIMED_ORDERS_DICT[relative_size],
                      'status' : "Init", 'crypto_type' : crypto_type}
        try:
            execute_size_coin = size_coin
            price_and_spread = None
            if relative_size:
                price_and_spread = self._orderbook['orderbook'].get_current_spread_and_price(crypto_type)

            if action_type == 'buy':
                if relative_size:
                    if price_and_spread['ask'] > price_fiat:
                        print("price is too high:",price_and_spread['ask'], "maximum price:",price_fiat)
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['ask'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    print("size:", size_coin, "execute_size:", execute_size_coin, "max_order_size", max_order_size)

                if execute_size_coin > 0:
                    order_info['crypto_size'] = execute_size_coin
                    print(datetime.datetime.now(), "Buying %f %s for %f" % (execute_size_coin, crypto_type, price_fiat))
                    sent_order = self._bitstamp_client.buy_limit_order(execute_size_coin, price_fiat,
                                                                       self.CRYPTO_CURRENCIES_DICT[crypto_type])
            elif action_type == 'sell':
                if relative_size:
                    if price_and_spread['bid'] < price_fiat:
                        execute_size_coin = 0
                    else:
                        execute_size_coin = min(size_coin, self._get_relative_size(price_and_spread['bid'],
                                                                                   self.ORDER_EXECUTION_MIN_FACTOR,
                                                                                   self.ORDER_EXECUTION_MAX_FACTOR))

                    execute_size_coin = self._get_order_size_limit(execute_size_coin, max_order_size)
                    print("size:", size_coin, "execute_size:", execute_size_coin, "max_order_size", max_order_size)

                if execute_size_coin > 0:
                    order_info['crypto_size'] = execute_size_coin
                    print(datetime.datetime.now(), "Selling %f %s with limit of %f" % (execute_size_coin, crypto_type, price_fiat))
                    sent_order = self._bitstamp_client.sell_limit_order(execute_size_coin, price_fiat,
                                                                        self.CRYPTO_CURRENCIES_DICT[crypto_type])
                    print ("sent order:",sent_order)

            if execute_size_coin > 0:
                print (datetime.datetime.now(), "Order sent in size", execute_size_coin, "for action", action_type)
        except (bitstamp.client.BitstampError, HTTPError) as e:
            order_info = None
            print(datetime.datetime.now(), "Order not sent:", str(e))
            sent_order = None

        order_done = False
        if sent_order is not None:
            order_id = sent_order.get("id")

            order_info['exchange_id'] = order_id
            print(datetime.datetime.now(), "order before cancel", sent_order)
            print(datetime.datetime.now(), "Getting status before cancel")
            order_status = None
            try:
                order_status = self._bitstamp_client.order_status(order_id)
                print("order status", order_status)
            except bitstamp.client.BitstampError as e:
                print("Can't get order status", str(e))

            if order_status is not None and order_status['status'] == 'Finished' and len(order_status['transactions']) > 0:
                order_info['status'] = order_status['status']
                order_info['price_fiat'] = order_status['transactions'][0]['price']
                order_done = True
            else:
                print(datetime.datetime.now(), "Cancelling order")
                try:
                    self._bitstamp_client.cancel_order(order_id)
                    order_info['status'] = 'Cancelled'
                    print(datetime.datetime.now(), "Order cancelled")
                except bitstamp.client.BitstampError as e:
                    order_info['status'] = 'Finished'
                    print(datetime.datetime.now(), "Exception while cancelling order:",str(e))
                    order_done = True
                    try:
                        found_transaction = False
                        all_transactions = self._bitstamp_client.user_transactions()
                        print("curr transaction", order_id, "transactions:",all_transactions)
                        for curr_transaction in all_transactions:
                            print(curr_transaction)
                            if curr_transaction['order_id'] == order_id or curr_transaction['order_id'] == int(order_id):
                                print("found price in transaction:", curr_transaction)
                                order_info['price_fiat'] = curr_transaction['btc_usd']
                                found_transaction = True
                                break
                        if not found_transaction:
                            print("Transaction for",order_id,"not found")

                    except (bitstamp.client.BitstampError, HTTPError) as e:
                        print(datetime.datetime.now(), "Exception while getting transactions data:", str(e))


        print (datetime.datetime.now(), "Order done, size is", execute_size_coin)
        if order_info is not None and execute_size_coin > 0:
            for curr_balance_key in self._balance_changed:
                self._balance_changed[curr_balance_key] = True

            order_info['balance'] = self._get_available_account_balance(crypto_type)
            self.write_order_to_db(order_info)
        return {'order_status' : order_done, 'execution_size' : execute_size_coin, 'execution_message' : execution_message}

    def _get_available_account_balance(self, crypto_type):
        account_balance = self.account_balance(crypto_type)
        crypto_key = ''
        for curr_account_balance_key in account_balance:
            if curr_account_balance_key.endswith("_available") and not curr_account_balance_key.startswith("usd"):
                crypto_key = curr_account_balance_key
                break

        if crypto_key != '':
            account_balance['crypto_available'] = account_balance[crypto_key]
        return account_balance

    def write_order_to_db(self, order_info):
        conn = self.create_db_connection(self._db_file)
        if conn is None:
            self.log.error("Can't connect to DB")
        else:
            try:
                insert_str = "INSERT INTO sent_orders VALUES('{}', '{}', {}, {}, {}, '{}', '{}', {}, '{}', {}, {})".format(order_info['exchange'], order_info['action_type'], order_info['crypto_size'],
                                             order_info['price_fiat'], order_info['exchange_id'], order_info['status'], order_info['order_time'], order_info['timed_order'], order_info['crypto_type'],
                                             order_info['balance']['usd_available'], order_info['balance']['crypto_available'])
                self.log.info(insert_str)
                conn.execute(insert_str)
                conn.commit()
            except Exception as e:
                print("error",str(e))

    def _get_relative_size(self,order_size, min_factor, max_factor):
        return random.uniform(min_factor, max_factor) * order_size

    def _get_order_size_limit(self, execute_size_coin, max_order_size):
        if max_order_size > 0 and execute_size_coin > max_order_size:
            execute_size_coin = self._get_relative_size(max_order_size, self.MAX_EXECUTION_MIN_FACTOR,
                                                        self.MAX_EXECUTION_MAX_FACTOR)
            execute_size_coin = float(min(Decimal(execute_size_coin).quantize(Decimal('1e-4')), max_order_size))
        return execute_size_coin

    def IsTimedOrderRunning(self):
        return self._is_timed_order_running

    def GetTimedOrderStatus(self):
        return {'timed_order_running': self.IsTimedOrderRunning(),
                'action_type': self._timed_order_action,
                'timed_order_required_size': self._timed_order_required_size,
                'timed_order_done_size': self._timed_order_done_size,
                'timed_order_sent_time': self._timed_order_start_time,
                'timed_order_execution_start_time': self._timed_order_execution_start_time,
                'timed_order_elapsed_time': self._timed_order_elapsed_time,
                'timed_order_duration_sec': self._timed_order_duration_sec,
                'timed_order_price_fiat': self._timed_order_price_fiat}

    def CancelTimedOrder(self):
        result = False
        if self._is_timed_order_running:
            self._is_timed_order_running = False
            result = True

        return result

    def GetSentOrders(self, orders_limit):
        conn = self.create_db_connection(self._db_file)
        limit_clause = ''
        if orders_limit > 0:
            limit_clause = " LIMIT " + str(orders_limit)
        sent_orders = conn.execute("SELECT * FROM (SELECT * FROM sent_orders ORDER BY datetime(order_time) DESC)" + limit_clause)
        all_orders = []
        for curr_order in sent_orders:
            exchange_id = curr_order[4]
            if exchange_id is None:
                exchange_id = ""
            order_dict = {'exchange' : curr_order[0],
                          'action_type' : curr_order[1],
                          'crypto_size': curr_order[2],
                          'price_fiat': curr_order[3],
                          'exchange_id': exchange_id,
                          'status': curr_order[5],
                          'order_time' : curr_order[6],
                          'timed_order' : curr_order[7],
                          'crypto_type': curr_order[8],
                          'usd_balance': curr_order[9],
                          'crypto_available': curr_order[10]}
            all_orders.append(order_dict)
        conn.close()
        return all_orders

    def set_client_credentails(self, client_credentials):
        username = ''
        key = ''
        secret = ''
        self.CancelTimedOrder()
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
        return {'signed_in_user': self._signed_in_user}

    def logout(self):
        self.set_client_credentails({})
        return self._bitstamp_client is None
