from client_wrapper_base import ClientWrapperBase
from timed_order_executer import TimedOrderExecuter
from orderbook_base import OrderbookFee
import logging
import copy
import datetime
import time
from decimal import Decimal
import random
import operator


class MultipleExchangesClientWrapper(ClientWrapperBase):
    MAXIMUM_ORDER_ATTEMPTS = 20
    ORDERBOOK_COMMANDS_FOR_ORDER = 20
    RATE_TIME_RATIO = 0.95
    TIMED_MAKE_PRICE_CHANGE_USD = 5
    TIMED_MAKE_SLEEP_INTERVAL_SEC = 30
    TIMED_MAKE_SLEEP_FACTOR = 0.75

    def __init__(self, clients, orderbook, db_interface, watchdog, sent_order_identifier, clients_manager):
        self._clients = clients
        super().__init__(orderbook, db_interface, clients_manager)
        self.log = logging.getLogger(__name__)
        self._watchdog = watchdog
        self._sent_order_identifier = sent_order_identifier

    def account_balance(self):
        self._last_balance = {'balances': dict()}
        for curr_client in self._clients:
            curr_account_balance = self._clients[curr_client].account_balance()
            if 'balances' in curr_account_balance:
                balances = curr_account_balance['balances']
                for curr_balance in balances:
                    if curr_balance not in self._last_balance['balances']:
                        self._last_balance['balances'][curr_balance] = copy.deepcopy(balances[curr_balance])
                    else:
                        for curr_key in ['amount', 'available']:
                            self._last_balance['balances'][curr_balance][curr_key] += \
                                balances[curr_balance][curr_key]

        self._last_balance['reserved_crypto'] = self._reserved_crypto
        self._last_balance['reserved_crypto_type'] = self._reserved_crypto_type
        self._last_balance['server_usd_reserved'] = self._reserved_usd
        self._last_balance['fees'] = self._orderbook.get_fees()
        return self._last_balance

    def send_immediate_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, relative_size,
                             max_order_size):
        remaining_size = size_coin
        remaining_execute_attempts = MultipleExchangesClientWrapper.MAXIMUM_ORDER_ATTEMPTS
        orderbook_type = ""
        if action_type == 'buy':
            orderbook_type = 'asks'
        elif action_type == 'sell':
            orderbook_type = 'bids'
        else:
            self.log.error("Unknown order type: <%s>", action_type)

        order_executed = False
        execution_messages = []
        exchanges_to_execute = {}
        total_execution_size = 0
        if orderbook_type != "":
            open_orders = self._orderbook.get_unified_orderbook\
                (crypto_type + "-USD", MultipleExchangesClientWrapper.ORDERBOOK_COMMANDS_FOR_ORDER,
                 OrderbookFee.TAKER_FEE)[orderbook_type]
            order_executed = False
            for curr_open_order in open_orders:
                exchange = curr_open_order['source']
                self.log.debug("Gathering order: type=<%s>, exchange=<%s> remaining_size=<%f>, order_size=<%f>, "
                               "price=<%f>, order_price=<%f>, remaining_attemprs=<%f>", action_type, exchange,
                               remaining_size, curr_open_order['size'], price_fiat, curr_open_order['price'],
                               remaining_execute_attempts)
                execute_size = min(remaining_size, curr_open_order['size'])
                if exchange not in exchanges_to_execute:
                    exchanges_to_execute[exchange] = execute_size
                else:
                    exchanges_to_execute[exchange] += execute_size
                remaining_size -= execute_size

                if remaining_size == 0:
                    break

            self.log.info("Command split to exchanges: <%s>", exchanges_to_execute)
            for exchange in exchanges_to_execute:
                client_for_order = self._clients[exchange]
                sent_order = client_for_order.send_immediate_order(action_type,
                                                                   exchanges_to_execute[exchange],
                                                                   crypto_type, price_fiat,
                                                                   fiat_type, relative_size, max_order_size)
                self.log.debug("Sent order to exchange <%s>: <%s>", exchange, sent_order)
                execution_messages.append(sent_order['execution_message'])
                if sent_order['execution_size'] > 0:
                    order_executed = True
                    total_execution_size += sent_order['execution_size']
        if order_executed:
            order_status = "Finished"
        else:
            order_status = "Cancelled"

        return {'execution_size': total_execution_size, 'execution_message': str(execution_messages),
                'order_status': order_status}

    def is_client_initialized(self):
        are_clients_init = True
        for client in self._clients:
            if not self._clients[client].is_client_initialized():
                are_clients_init = False
                break
        return are_clients_init

    def send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size,
                   report_status):
        self._watchdog.register_orderbook(self._sent_order_identifier, self._orderbook)
        return super().send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec,
                                  max_order_size, report_status)

    def _order_complete(self, is_timed_order, report_status):
        self._watchdog.unregister_orderbook(self._sent_order_identifier)
        self._clients_manager.unregister_client(self._sent_order_identifier)
        super()._order_complete(is_timed_order, report_status)

    def get_exchange_name(self):
        all_names = ""
        for curr_client in self._clients:
            if all_names == "":
                all_names = curr_client
            else:
                all_names = all_names + ", " + curr_client
        return all_names

    def _create_timed_order_executer(self, asset_pair, action_type):
        orders = self._orderbook.get_unified_orderbook(asset_pair, 1, OrderbookFee.TAKER_FEE)
        exchange = ""
        executer = None
        if action_type == 'buy' and len(orders['asks']) > 0:
            exchange = orders['asks'][0]['source']
        elif action_type == 'sell' and len(orders['bids']) > 0:
            exchange = orders['bids'][0]['source']

        if exchange != "":
            print("Creating timed executer for exchange {}".format(exchange))
            executer = TimedOrderExecuter(self._clients[exchange], {'orderbook': self._orderbook}, asset_pair)
        return executer

    def _execute_timed_make_order_in_thread(self, action_type, size_coin, crypto_type, price_fiat, fiat_type,
                                            duration_sec, max_order_size, max_relative_spread_factor,
                                            relative_to_best_order_ratio, report_status):
        self.log.debug("executing timed make order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'crypto_size': size_coin,
                      'price_fiat' : price_fiat, 'exchange_id': 0, 'order_time' : order_time,
                      'timed_order': self.TIMED_ORDERS_DICT[True], 'status': "Timed Order", 'crypto_type': crypto_type,
                      'balance': self.account_balance()}
        self.log.info("order info before execution: <%s>", order_info)
        self._db_interface.write_order_to_db(order_info)
        self._reserved_crypto_type = crypto_type
        self._timed_order_action = action_type
        self._timed_order_price_fiat = price_fiat
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self._timed_order_execution_start_time = ''
        self._timed_order_required_size = size_coin
        asset_pair = crypto_type + "-" + fiat_type
        self._timed_order_done_size = 0
        self._timed_order_elapsed_time = 0
        self._timed_order_duration_sec = duration_sec
        timed_order_start_time = time.time()
        start_timestamp = datetime.datetime.utcnow()
        self._timed_order_execution_start_time = time.time()#start_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        for exchange in self._clients:
            client_for_order = self._clients[exchange]
            client_for_order.set_timed_command_listener(self)
        prev_time = 0
        curr_rate = 0
        limit_price_difference = 0
        prev_run_size = 0
        while self.is_timed_order_running():
            remaining_size = self._timed_order_required_size - self._timed_order_done_size
            curr_time = time.time()
            curr_run_size = 0
            if prev_time == 0:
                if action_type == 'sell_limit':
                    limit_price_difference = -1 * MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
                elif action_type == 'buy_limit':
                    limit_price_difference = MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
            else:
                for exchange in self._clients:
                    client_for_order = self._clients[exchange]
                    client_status = client_for_order.get_timed_order_status()
                    curr_run_size += client_status['timed_order_done_size']
                    client_for_order.cancel_timed_order()
                curr_run_size -= prev_run_size
                prev_run_size = curr_run_size
                for exchange in self._clients:
                    client_for_order = self._clients[exchange]
                    client_for_order.join_timed_make_thread()

                self._timed_order_elapsed_time = time.time() - timed_order_start_time
                time_from_prev_time = prev_time - curr_time
                required_rate = remaining_size / (self._timed_order_duration_sec - self._timed_order_elapsed_time)
                curr_rate = curr_rate * MultipleExchangesClientWrapper.RATE_TIME_RATIO + \
                            (1 - MultipleExchangesClientWrapper.RATE_TIME_RATIO) * curr_run_size / time_from_prev_time
                if curr_rate > required_rate:
                    limit_price_difference += MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
                    print("Current rate is too fast, increasing price difference to {}".format(
                        limit_price_difference))
                elif curr_rate < required_rate:
                    limit_price_difference -= MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
                    print("Current rate is too slow, decreasing price difference to {}".format(
                        limit_price_difference))
            client_prices = dict()
            for exchange in self._clients:
                client_for_order = self._clients[exchange]
                client_price = client_for_order.get_orderbook_price(asset_pair)
                sort_factor = 0
                if client_price:
                    price_type = ""
                    if action_type == 'sell_limit':
                        price_type = 'ask'
                        sort_factor = 1
                    elif action_type == 'buy_limit':
                        price_type = 'bid'
                        sort_factor = -1
                    client_price_usd = client_price[price_type]['price']
                    client_balance = client_for_order.account_balance()
                    print("Balance for {}: {}".format(exchange, client_balance))
                    client_usd_balance = 0
                    if 'balances' in client_balance and 'USD' in client_balance['balances']:
                        client_usd_balance = client_balance['balances']['USD']['available']
                    client_crypto_balance = 0
                    if 'balances' in client_balance and crypto_type.upper() in client_balance['balances']:
                        client_crypto_balance = client_balance['balances'][crypto_type.upper()]['available']
                    client_prices[exchange] = {'client': client_for_order,
                                               'price': client_price_usd * sort_factor,
                                               'exchange': exchange,
                                               'minimum_order_size': client_for_order.minimum_order_size(asset_pair),
                                               'usd_balance': client_usd_balance,
                                               'crypto_balance': client_crypto_balance}
            min_order_sorted_clients = sorted(client_prices.values(), key=operator.itemgetter('minimum_order_size'))
            print("Order size sorted clients: {}".format(min_order_sorted_clients))
            if remaining_size > 0:
                num_of_exchanges = 1
                orders_big_enough = False
                exchange_size = remaining_size
                change_sizes_for_balance = False
                while not orders_big_enough and self.is_timed_order_running():
                    num_of_exchanges = len(min_order_sorted_clients)
                    exchange_size = float(Decimal(remaining_size / num_of_exchanges).quantize(Decimal('1e-4')))
                    orders_big_enough = True
                    remove_exchange = None
                    for exchange_index in range(num_of_exchanges):
                        if min_order_sorted_clients[exchange_index]['client'].minimum_order_size(asset_pair) > \
                                exchange_size:
                            orders_big_enough = False
                            remove_exchange = exchange_index
                            break
                        elif not change_sizes_for_balance and action_type == 'sell_limit' and \
                                min_order_sorted_clients[exchange_index]['crypto_balance'] < \
                                exchange_size or action_type == 'buy_limit' and \
                                min_order_sorted_clients[exchange_index]['usd_balance'] / \
                                abs(min_order_sorted_clients[exchange_index]['price']) < exchange_size:
                            change_sizes_for_balance = True

                    if not orders_big_enough and len(min_order_sorted_clients) == 1:
                        self._is_timed_order_running = False
                        print("Order size is too small for all exchanges, cancelling")
                    elif not orders_big_enough:
                        print("Removing exchange {} because its minimum size is too small".format
                              (min_order_sorted_clients[remove_exchange]['exchange']))
                        del min_order_sorted_clients[len(min_order_sorted_clients) - 1]
                num_of_exchanges = len(min_order_sorted_clients)
                if not change_sizes_for_balance:
                    for client_index in range(num_of_exchanges):
                        min_order_sorted_clients[client_index]['execute_size'] = exchange_size
                elif change_sizes_for_balance and action_type == 'sell_limit':
                    balance_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('crypto_balance'))
                    balance_for_order = remaining_size
                    exchange_size = float(Decimal(balance_for_order / num_of_exchanges).quantize(Decimal('1e-4')))
                    print("{} active exchanges, remaining size={}, exchange_size={}, clients={}".format(
                        num_of_exchanges, balance_for_order, exchange_size, balance_sorted_clients))
                    for client_index in range(num_of_exchanges):
                        if exchange_size <= balance_sorted_clients[client_index]['crypto_balance']:
                            print("Setting execute size to {} for exchange {}".format(
                                exchange_size, balance_sorted_clients[client_index]))
                            balance_sorted_clients[client_index]['execute_size'] = exchange_size
                        else:
                            balance_sorted_clients[client_index]['execute_size'] = \
                                balance_sorted_clients[client_index]['crypto_balance']
                            print("Low balance, setting execute size to {} for exchange {}".format(
                                exchange_size, balance_sorted_clients[client_index]))
                        print("Going to execute {} in exchange {}".format(
                            balance_sorted_clients[client_index]['execute_size'],
                            balance_sorted_clients[client_index]['exchange']))
                        balance_for_order -= balance_sorted_clients[client_index]['execute_size']
                        if client_index < num_of_exchanges - 1:
                            exchange_size = float(
                                Decimal(balance_for_order / (
                                        num_of_exchanges - client_index - 1)).quantize(Decimal('1e-4')))
                    if balance_for_order > 0.0001:
                        print("Not enough available balance in the exchanges for executing the order, "
                              "missing {}".format(balance_for_order))
                        self._is_timed_order_running = False
                elif change_sizes_for_balance and action_type == 'buy_limit':
                    balance_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('usd_balance'))
                    balance_for_order = remaining_size
                    exchange_size = float(Decimal(balance_for_order / num_of_exchanges).quantize(Decimal('1e-4')))
                    for client_index in range(num_of_exchanges):
                        if exchange_size <= balance_sorted_clients['usd_balance'] / price_fiat:
                            balance_sorted_clients[client_index]['execute_size'] = exchange_size
                        else:
                            size_for_client = float(Decimal(
                                balance_sorted_clients['usd_balance'] / price_fiat).quantize(Decimal('1e-4')))
                            balance_sorted_clients[client_index]['execute_size'] = size_for_client
                        print("Going to execute {} in exchange {}".format(
                            balance_sorted_clients[client_index]['execute_size'],
                            balance_sorted_clients[client_index]['exchange']))
                        balance_for_order -= balance_sorted_clients[client_index]['execute_size']
                        if client_index < num_of_exchanges - 1:
                            exchange_size = float(
                                Decimal(balance_for_order / (
                                        num_of_exchanges - client_index + 1)).quantize(Decimal('1e-4')))
                    if balance_for_order > 0:
                        print("Not enough available usd in the exchanges for executing the order")
                        self._is_timed_order_running = False

                if self.is_timed_order_running():
                    print("Min order sorted clients: ", min_order_sorted_clients)
                    price_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('price'))
                    #print("num_of_exchanges", num_of_exchanges, "remaining_size", remaining_size, "max_order_size", max_order_size)
                    #num_of_exchanges = max(num_of_exchanges, 1)
                    best_price = abs(price_sorted_clients[0]['price'])
                    if action_type == 'sell_limit':
                        best_price += limit_price_difference
                        best_price = max(best_price, price_fiat)
                    elif action_type == 'buy_limit':
                        best_price -= limit_price_difference
                        best_price = min(best_price, price_fiat)
                    print("Going to execute in {} exchanges".format(num_of_exchanges))
                    for exchange_index in range(num_of_exchanges):
                        print("Executing <{}> in exchange <{}> for <{}> USD".format(price_sorted_clients[exchange_index]
                                                                                    ['execute_size'],
                                                                                    price_sorted_clients[exchange_index]
                                                                                    ['exchange'],
                                                                                    best_price))
                        price_sorted_clients[exchange_index]['client'].send_order(action_type,
                                                                                  price_sorted_clients[exchange_index]
                                                                                  ['execute_size'], crypto_type,
                                                                                  best_price, fiat_type, duration_sec,
                                                                                  max_order_size, False)
            else:
                self._is_timed_order_running = False

            if self.is_timed_order_running():
                sleep_interval = random.uniform(MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_FACTOR *
                                                MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_INTERVAL_SEC,
                                                MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_INTERVAL_SEC)
                print("Sleeping for {} seconds".format(sleep_interval))
                time.sleep(sleep_interval)
                prev_time = curr_time
        for exchange in self._clients:
            client_for_order = self._clients[exchange]
            client_for_order.set_timed_command_listener(None)
            client_for_order.cancel_timed_order()

        for exchange in self._clients:
            client_for_order = self._clients[exchange]
            client_for_order.join_timed_make_thread()

        self._order_complete(True, True)