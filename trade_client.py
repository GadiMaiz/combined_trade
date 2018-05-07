import bitstamp.client

class TradeClient:
    def __init__(self, credentials = []):
        self.exchange_accounts = []
        self.exchange_clients = []
        self.orders = []
        exchange_creators = {'bitstamp' : create_bitstamp_client }

        for curr_credentials in credentials:
            curr_exchange = curr_credentials['exchange_name']
            if curr_exchange in exchange_creators:
                client_creator = exchange_creators[curr_exchange]
                curr_client = client_creator(curr_credentials)
                self.exchange_clients.append(curr_client)

    def create_bitstamp_client(self, bitstamp_credentials):
        bitstamp_client = []
        username = bitstamp_credentials['username']
        key = bitstamp_credentials['key']
        secret = bitstamp_credentials['secret']
        if (len(username) != 0 and len(username) != 0 and len(username) != 0):
            bitstamp_client = bitstamp.client.Trading(username=username, key=key, secret=secret)

        return bitstamp_client

    def send_limit_order(self, action, type, exchange, amount, price, base, quote):
        result = []
        if exchange in self.exchange_clients:
            trade_client = self.exchange_clients[exchange]
            result = action(amount, price, base, quote)
            if "id" in result:
                result["exchange"] = exchange
                result["status"] = "SENT"
                result["action_type"] = type
                self.orders[exchange + "_" + result["id"]] = result

        return result

    def sell_limit_order(self, exchange, amount, price, base="btc", quote="usd"):
        return send_limit_order(self.sell, "SELL", exchange, amount, price, base, quote)

    def buy_limit_order(self, exchange, amount, price, base="btc", quote="usd"):
        return send_limit_order(self.buy, "BUY", exchange, amount, price, base, quote)

    def update_order_status(self, global_order_id):
        found_order = []
        order_key = global_order_id['exchange'] + "_" + global_order_id['order_id']
        if order_key in self.orders:
            found_order = self.orders[order_key]
            if found_order["status"] == "SENT":
                order_client = self.exchange_clients[found_order["exchange"]]
                open_orders = order_client.open_orders()
                found_open_order = []
                for curr_order in open_orders
                    if curr_order["id"] == order_id:
                        found_open_order = curr_order
                        break

                if len(found_open_order) == 0:
                    found_order["status"] = "DONE"

        return found_order

    def cancel_order(self, global_order_id):
        curr_order = update_order_status(global_order_id)
        if len(curr_order) > 0:
            if curr_order["status"] == "SENT":
                order_client = self.exchange_clients[curr_order["exchange"]]
                order_client.cancel_order(curr_order["id"])
                curr_order["status"] = "CANCELLED"

        return curr_order

    def account_balance(self):
        account_balances = []
        for curr_exchange in self.exchange_clients:
            account_balances.append(curr_exchange.account_balance())

        return account_balances

    def open_orders(self):
        all_open_orders = []
        for curr_exchange in self.exchange_clients:
            all_open_orders.append(curr_exchange.open_orders())

        return all_open_orders