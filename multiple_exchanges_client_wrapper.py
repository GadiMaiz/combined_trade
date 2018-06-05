from client_wrapper_base import ClientWrapperBase
import logging
import copy

class MultipleExchangesClientWrapper(ClientWrapperBase):
    MAXIMUM_ORDER_ATTEMPTS = 20
    ORDERBOOK_COMMANDS_FOR_ORDER = 5
    def __init__(self, clients, orderbook, db_interface, watchdog, sent_order_identifier):
        self._clients = clients
        super().__init__(orderbook, db_interface)
        self.log = logging.getLogger(__name__)
        self._watchdog = watchdog
        self._sent_order_identifier = sent_order_identifier
    """def can_send_order(self, action_type, size_coin, crypto_type, price_fiat, fiat_type, duration_sec):
        result, refuse_reason = ClientWrapperBase.verify_order_params(size_coin, price_fiat, duration_sec)

        if result:
            currency = None
            if action_type == 'buy':
                currency = 'USD'
            else:
                currency = crypto_type

            

        return {'can_send_order': result, 'reason': refuse_reason}"""

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

        order_executed = True
        execution_messages = []
        if orderbook_type != "":
            while remaining_size > 0 and remaining_execute_attempts > 0 and order_executed:
                remaining_execute_attempts -= 1
                open_orders = self._orderbook.get_unified_orderbook\
                    (crypto_type + "-USD", MultipleExchangesClientWrapper.ORDERBOOK_COMMANDS_FOR_ORDER)[orderbook_type]
                order_executed = False
                for curr_open_order in open_orders:
                    exchange = curr_open_order['source']
                    self.log.debug("Sending order: type=<%s>, exchange=<%s> remaining_size=<%f>, order_size=<%f>, "
                                   "price=<%f>, order_price=<%f>, remaining_attemprs=<%f>", action_type, exchange,
                                   remaining_size, curr_open_order['size'], price_fiat, curr_open_order['price'],
                                   remaining_execute_attempts)
                    client_for_order = self._clients[curr_open_order['source']]
                    sent_order = client_for_order.send_immediate_order(action_type,
                                                                       min(remaining_size, curr_open_order['size']),
                                                                       crypto_type, price_fiat,
                                                                       fiat_type, relative_size, max_order_size)
                    self.log.debug("Sent order: <%s>", sent_order)
                    execution_messages.append(sent_order['execution_message'])
                    if sent_order['execution_size'] > 0:
                        order_executed = True
                        remaining_size -= sent_order['execution_size']
                        break

                if not order_executed:
                    self.log.error("No order from orderbook succeeded. Commands are: <%s>", open_orders)

        order_status = "Finished"
        if remaining_size == size_coin:
            order_status = "Cancelled"

        return {'execution_size': size_coin - remaining_size, 'execution_message': str(execution_messages),
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