import bitfinex

class BitfinexClient(bitfinex.TradeClient):
    def __init__(self, key, secret):
        super().__init__(key, secret)

    def account_balance(self):
        return super().balances()


client = BitfinexClient("3x7G37PqF7KtcFAVXrUeOaBBzJNCpho4VdVugeAw3iX",
                        "d1FRlw7vPQvF0M2RNnbhSrHiAU6V6PBtKaVKNLYIOe0")

print(client.account_balance())

"""result = client.place_order("0.002", "7800", "sell", "exchange fill-or-kill", "btcusd")
print("result:", result)
order_status = client.status_order(result['id'])
print ("status:", order_status)
print(client.account_balance())"""

transactions = client.past_trades(0, "btcusd")
transactions = transactions + client.past_trades(0, "bchusd")
print(transactions)