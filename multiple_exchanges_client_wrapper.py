from client_wrapper_base import ClientWrapperBase
from timed_order_executer import TimedOrderExecuter
import logging
import copy


class MultipleExchangesClientWrapper(ClientWrapperBase):
    MAXIMUM_ORDER_ATTEMPTS = 20
    ORDERBOOK_COMMANDS_FOR_ORDER = 20

    def __init__(self, clients, orderbook, db_interface, watchdog, sent_order_identifier):
        self._clients = clients
        super().__init__(orderbook, db_interface)
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
        self._last_balance['fee'] = self.exchange_fee("BTC")
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
                (crypto_type + "-USD", MultipleExchangesClientWrapper.ORDERBOOK_COMMANDS_FOR_ORDER)[orderbook_type]
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
        print(are_clients_init)
        return are_clients_init

    def send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size):
        self._watchdog.register_orderbook(self._sent_order_identifier, self._orderbook)
        return super().send_order(action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec, max_order_size)

    def _order_complete(self):
        self._watchdog.unregister_orderbook(self._sent_order_identifier)

    def get_exchange_name(self):
        all_names = ""
        for curr_client in self._clients:
            if all_names == "":
                all_names = curr_client
            else:
                all_names = all_names + ", " + curr_client
        return all_names

    def _create_timed_order_executer(self, asset_pair, action_type):
        orders = self._orderbook.get_unified_orderbook(asset_pair, 1)
        exchange = ""
        executer = None
        if action_type == 'buy' and len(orders['asks']) > 0:
            exchange = orders['asks'][0]['source']
        elif action_type == 'sell' and len(orders['bids']) > 0:
            exchange = orders['bids'][0]['source']

        if exchange != "":
            executer = TimedOrderExecuter(self._clients[exchange], self._orderbook, asset_pair)
        return executer
