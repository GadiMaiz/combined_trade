import bitfinex

class BitfinexClient(bitfinex.TradeClient):
    def __init__(self, key, secret):
        super().__init__(key, secret)

    def account_balance(self):
        return super().balances()


client = BitfinexClient("3x7G37PqF7KtcFAVXrUeOaBBzJNCpho4VdVugeAw3iX",
                        "d1FRlw7vPQvF0M2RNnbhSrHiAU6V6PBtKaVKNLYIOe0")

print(client.account_balance())