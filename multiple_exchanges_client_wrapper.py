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

    def __init__(self, clients, orderbook, db_interface, watchdog, sent_order_identifier, clients_manager, account):
        self._clients = clients
        super().__init__(orderbook, db_interface, clients_manager, account)
        self.log = logging.getLogger('smart-trader')
        self._watchdog = watchdog
        self._sent_order_identifier = sent_order_identifier

    def account_balance(self, reconnect=False, extended_info=True):
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
        self._last_balance['fees'] = self._orderbook['orderbook'].get_fees()
        return self._last_balance

    def send_immediate_order(self, action_type, size_coin, currency_to, price, currency_from, relative_size,
                             max_order_size, is_timed_order, parent_trade_order_id, external_order_id, user_quote_price,
                             user_id):
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
            open_orders = self._orderbook['orderbook'].get_unified_orderbook\
                (currency_to + "-" + currency_from, MultipleExchangesClientWrapper.ORDERBOOK_COMMANDS_FOR_ORDER,
                 OrderbookFee.TAKER_FEE)[orderbook_type]
            order_executed = False
            for curr_open_order in open_orders:
                exchange = curr_open_order['source']
                self.log.debug("Gathering order: type=<%s>, exchange=<%s> remaining_size=<%f>, order_size=<%f>, "
                               "price=<%f>, order_price=<%f>, remaining_attemprs=<%f>", action_type, exchange,
                               remaining_size, curr_open_order['size'], price, curr_open_order['price'],
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
                                                                   currency_to, price,
                                                                   currency_from, relative_size, max_order_size,
                                                                   is_timed_order, parent_trade_order_id,
                                                                   external_order_id, user_quote_price, user_id)
                self.log.debug("Sent order to exchange <%s>: <%s>", exchange, sent_order)
                if sent_order['execution_message'] != '':
                    execution_messages.append(sent_order['execution_message'])
                if sent_order['execution_size'] > 0:
                    order_executed = True
                    self.log.debug("Order executed: <%f>", sent_order['execution_size'])
                    total_execution_size += sent_order['execution_size']
        if order_executed:
            order_status = "True"
        else:
            order_status = "False"

        return {'execution_size': total_execution_size, 'execution_message': execution_messages,
                'order_status': order_status}

    def is_client_initialized(self):
        are_clients_init = True
        for client in self._clients:
            if not self._clients[client].is_client_initialized():
                are_clients_init = False
                break
        return are_clients_init

    def send_order(self, action_type, size_coin, currency_to, price, currency_from, duration_sec, max_order_size,
                   report_status, external_order_id, user_quote_price, user_id, parent_order_id=-1,
                   max_exchange_sizes=dict(), order_listener=None):
        self._watchdog.register_orderbook(self._sent_order_identifier, self._orderbook['orderbook'])
        return super().send_order(action_type, size_coin, currency_to, price, currency_from, duration_sec,
                                  max_order_size, report_status, external_order_id, user_quote_price, user_id,
                                  parent_order_id, max_exchange_sizes, order_listener)

    def _order_complete(self, is_timed_order, report_status, currency_to, external_order_id):
        self._watchdog.unregister_orderbook(self._sent_order_identifier)
        self._clients_manager.unregister_client(self._sent_order_identifier, external_order_id)
        super()._order_complete(is_timed_order, report_status, currency_to, external_order_id)

    def get_exchange_name(self):
        all_names = ""
        for curr_client in self._clients:
            if all_names == "":
                all_names = curr_client
            else:
                all_names = all_names + ", " + curr_client
        return all_names

    def _create_timed_order_executer(self, asset_pair, action_type, max_exchange_sizes, done_size_exchanges):
        orders = self._orderbook['orderbook'].get_unified_orderbook(asset_pair, 1, OrderbookFee.TAKER_FEE,
                                                                    done_size_exchanges)
        self.log.debug("Orders: <%s>", orders)
        exchange = ""
        executer = None
        if action_type == 'buy' and len(orders['asks']) > 0:
            exchange = orders['asks'][0]['source']
        elif action_type == 'sell' and len(orders['bids']) > 0:
            exchange = orders['bids'][0]['source']

        if exchange != "":
            self.log.debug("Creating timed executer for exchange <%s>", exchange)
            executer = TimedOrderExecuter(self._clients[exchange], {'orderbook': self._orderbook['orderbook']},
                                          asset_pair)
        return executer

    def _execute_timed_make_order_in_thread(self, action_type, size_coin, currency_from, currency_to, price,
                                            duration_sec, max_order_size, max_relative_spread_factor,
                                            relative_to_best_order_ratio, report_status, external_order_id,
                                            user_quote_price, user_id, parent_trade_order_id, max_exchange_sizes,
                                            order_listener):
        self.log.debug("executing timed make order")
        order_timestamp = datetime.datetime.utcnow()
        (dt, micro) = order_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
        order_time = "%s.%02d" % (dt, int(micro) / 1000)
        order_info = {'exchange': self.get_exchange_name(), 'action_type': action_type, 'size': size_coin,
                      'price' : price, 'exchange_id': 0, 'order_time' : order_time,
                      'timed_order': self.TIMED_ORDERS_DICT[True], 'status': "Timed Order", 'currency_to': currency_to,
                      'currency_from': currency_from,
                      'balance': self.account_balance(), 'external_order_id': external_order_id,
                      'user_quote_price': user_quote_price, 'user_id': user_id, 'account': self._account}
        self.log.info("order info before execution: <%s>", order_info)
        db_trade_order_id = self._db_interface.write_order_to_db(order_info)
        if parent_trade_order_id == -1:
            parent_trade_order_id = db_trade_order_id
        self._reserved_crypto_type = currency_from
        timed_order = self._timed_orders[currency_to]
        timed_order.action = action_type
        timed_order.price = price
        timed_order.start_time = time.time()
        timed_order.execution_start_time = ''
        timed_order.required_size = size_coin
        asset_pair = currency_to + "-" + currency_from
        timed_order.done_size = 0
        timed_order.elapsed_time = 0
        timed_order.duration_sec = duration_sec
        timed_order_start_time = time.time()
        timed_order.execution_start_time = time.time()
        for exchange in self._clients:
            client_for_order = self._clients[exchange]
        prev_time = 0
        curr_rate = 0
        limit_price_difference = 0
        prev_run_size = 0
        prev_balances = {}
        while timed_order.running:
            remaining_size = timed_order.required_size - timed_order.done_size
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
                    client_status = client_for_order.get_timed_order_status(currency_to)
                    curr_run_size += client_status['timed_order_done_size']
                    client_for_order.cancel_timed_order()
                curr_run_size -= prev_run_size
                prev_run_size = curr_run_size
                for exchange in self._clients:
                    client_for_order = self._clients[exchange]
                    client_for_order.join_timed_make_thread(currency_to)

                timed_order.elapsed_time = time.time() - timed_order_start_time
                time_from_prev_time = prev_time - curr_time
                required_rate = remaining_size / (timed_order.duration_sec - timed_order.elapsed_time)
                curr_rate = curr_rate * MultipleExchangesClientWrapper.RATE_TIME_RATIO + \
                            (1 - MultipleExchangesClientWrapper.RATE_TIME_RATIO) * curr_run_size / time_from_prev_time

                if curr_rate < required_rate or timed_order.elapsed_time > duration_sec:
                    limit_price_difference -= MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
                    self.log.debug("Current rate is too slow, decreasing price difference to <%f>",
                                   limit_price_difference)
                elif curr_rate > required_rate:
                    limit_price_difference += MultipleExchangesClientWrapper.TIMED_MAKE_PRICE_CHANGE_USD
                    self.log.debug("Current rate is too fast, increasing price difference to <%f>",
                                   limit_price_difference)
            client_prices = dict()
            for exchange in self._clients:
                client_for_order = self._clients[exchange]
                client_price = client_for_order.get_orderbook_price(asset_pair)
                self.log.debug("Making in exchange <%s> with prices <%s>", exchange, str(client_price))
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
                    self.log.debug("Balance for <%s>: <%s>", exchange, str(client_balance))
                    client_usd_balance = 0
                    if 'balances' in client_balance and 'USD' in client_balance['balances']:
                        client_usd_balance = client_balance['balances']['USD']['available']
                    client_crypto_balance = 0
                    if 'balances' in client_balance and currency_to.upper() in client_balance['balances']:
                        client_crypto_balance = client_balance['balances'][currency_to.upper()]['available']
                    prev_balance_difference = 0
                    if exchange in prev_balances:
                        prev_balance_difference = prev_balances[exchange] - client_crypto_balance
                    prev_balances[exchange] = client_crypto_balance
                    if exchange in max_exchange_sizes:
                        max_exchange_sizes[exchange] -= prev_balance_difference
                        self.log.debug("Max order size for exchange <%s> is <%f>", exchange,
                                       max_exchange_sizes[exchange])
                        client_crypto_balance = min(client_crypto_balance, max_exchange_sizes[exchange])
                    else:
                        self.log.debug("No max size for exchange <%s>", exchange)
                    client_prices[exchange] = {'client': client_for_order,
                                               'price': client_price_usd * sort_factor,
                                               'exchange': exchange,
                                               'minimum_order_size': client_for_order.minimum_order_size(asset_pair),
                                               'usd_balance': client_usd_balance,
                                               'crypto_balance': client_crypto_balance}
            min_order_sorted_clients = sorted(client_prices.values(), key=operator.itemgetter('minimum_order_size'))
            self.log.debug("Order size sorted clients: <%s>", str(min_order_sorted_clients))
            if remaining_size <= ClientWrapperBase.MINIMUM_REMAINING_SIZE:
                timed_order.running = False
                order_info['status'] = 'Make Order Finished'
            else:
                num_of_exchanges = 1
                orders_big_enough = False
                exchange_size = remaining_size
                change_sizes_for_balance = False
                while not orders_big_enough and timed_order.running:
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
                        timed_order.running = False
                        self.log.info("Order size <%f> is too small for all exchanges, cancelling", remaining_size)
                    elif not orders_big_enough:
                        self.log.debug("Removing exchange <%s> because its minimum size is too small",
                                       min_order_sorted_clients[remove_exchange]['exchange'])
                        del min_order_sorted_clients[len(min_order_sorted_clients) - 1]
                num_of_exchanges = len(min_order_sorted_clients)
                if not change_sizes_for_balance:
                    for client_index in range(num_of_exchanges):
                        min_order_sorted_clients[client_index]['execute_size'] = exchange_size
                elif change_sizes_for_balance and action_type == 'sell_limit':
                    balance_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('crypto_balance'))
                    balance_for_order = remaining_size
                    exchange_size = float(Decimal(balance_for_order / num_of_exchanges).quantize(Decimal('1e-4')))
                    self.log.debug("<%d> active exchanges, remaining size=<%f>, exchange_size=<%f>, clients=<%s>",
                                   num_of_exchanges, balance_for_order, exchange_size, str(balance_sorted_clients))
                    for client_index in range(num_of_exchanges):
                        if exchange_size <= balance_sorted_clients[client_index]['crypto_balance']:
                            self.log.debug("Setting execute size to <%f> for exchange <%s>", exchange_size,
                                           balance_sorted_clients[client_index])
                            balance_sorted_clients[client_index]['execute_size'] = exchange_size
                        else:
                            balance_sorted_clients[client_index]['execute_size'] = \
                                balance_sorted_clients[client_index]['crypto_balance']
                            self.log.debug("Low balance, setting execute size to <%f> for exchange <%s>",
                                           exchange_size, balance_sorted_clients[client_index])
                        balance_for_order -= balance_sorted_clients[client_index]['execute_size']
                        if client_index < num_of_exchanges - 1:
                            exchange_size = float(
                                Decimal(balance_for_order / (
                                        num_of_exchanges - client_index - 1)).quantize(Decimal('1e-4')))
                    if balance_for_order > 0.0001:
                        self.log.debug("Not enough available balance in the exchanges for executing the order, "
                                       "missing <%f>", balance_for_order)
                        timed_order.order_running = False
                        order_info['status'] = 'Make Order Incomplete'
                elif change_sizes_for_balance and action_type == 'buy_limit':
                    balance_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('usd_balance'))
                    balance_for_order = remaining_size
                    exchange_size = float(Decimal(balance_for_order / num_of_exchanges).quantize(Decimal('1e-4')))
                    for client_index in range(num_of_exchanges):
                        if exchange_size <= balance_sorted_clients[client_index]['usd_balance'] / price:
                            balance_sorted_clients[client_index]['execute_size'] = exchange_size
                        else:
                            size_for_client = float(Decimal(
                                balance_sorted_clients[client_index]['usd_balance'] / price).quantize(
                                Decimal('1e-4')))
                            balance_sorted_clients[client_index]['execute_size'] = size_for_client
                        self.log.debug("Going to execute <%f> in exchange <%s>",
                                       balance_sorted_clients[client_index]['execute_size'],
                                       balance_sorted_clients[client_index]['exchange'])
                        balance_for_order -= balance_sorted_clients[client_index]['execute_size']
                        if client_index < num_of_exchanges - 1:
                            exchange_size = float(
                                Decimal(balance_for_order / (
                                        num_of_exchanges - client_index - 1)).quantize(Decimal('1e-4')))
                    if balance_for_order > 0.0001:
                        self.log.debug("Not enough available usd in the exchanges for executing the order")
                        self._is_timed_order_running = False
                        order_info['status'] = 'Make Order Incomplete'

                if timed_order.running:
                    self.log.debug("Min order sorted clients: <%s>", min_order_sorted_clients)
                    price_sorted_clients = sorted(min_order_sorted_clients, key=operator.itemgetter('price'))
                    best_price = abs(price_sorted_clients[0]['price'])
                    if action_type == 'sell_limit':
                        best_price += limit_price_difference
                        best_price = max(best_price, price)
                    elif action_type == 'buy_limit':
                        best_price -= limit_price_difference
                        best_price = min(best_price, price)
                    self.log.debug("Going to execute in <%d> exchanges", num_of_exchanges)
                    for exchange_index in range(num_of_exchanges):
                        self.log.debug("Executing <%f> in exchange <%s> for <%f> USD",
                                       price_sorted_clients[exchange_index]['execute_size'],
                                       price_sorted_clients[exchange_index]['exchange'], best_price)
                        price_sorted_clients[exchange_index]['client'].send_order(action_type,
                                                                                  price_sorted_clients[exchange_index]
                                                                                  ['execute_size'], currency_to,
                                                                                  best_price, currency_from,
                                                                                  duration_sec, max_order_size, False,
                                                                                  external_order_id, user_quote_price,
                                                                                  user_id, parent_trade_order_id,
                                                                                  dict(), { 'listener': self,
                                                                                  'order': timed_order})

            if timed_order.running:
                sleep_interval = random.uniform(MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_FACTOR *
                                                MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_INTERVAL_SEC,
                                                MultipleExchangesClientWrapper.TIMED_MAKE_SLEEP_INTERVAL_SEC)
                self.log.debug("Sleeping for <%f> seconds", sleep_interval)
                self._cancel_event.wait(sleep_interval)
                #time.sleep(sleep_interval)
                prev_time = curr_time
        for exchange in self._clients:
            client_for_order = self._clients[exchange]
            client_for_order.cancel_timed_order()

        for exchange in self._clients:
            client_for_order = self._clients[exchange]
            client_for_order.join_timed_make_thread(currency_to)

        self._db_interface.write_order_to_db(order_info)
        self._order_complete(True, True, currency_to, external_order_id)
