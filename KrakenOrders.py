import json
from bitex.api.REST import KrakenREST

class KrakenQuery():
    def get_current_partial_book(self, asset_pair, size):
        kraken_pair = asset_pair
        if asset_pair == 'BTC-USD':
            kraken_pair = 'XXBTZUSD'

        k = KrakenREST()
        orders_bytes = k.query('GET', 'public/Depth', params={'pair': kraken_pair})
        orders = json.loads(orders_bytes.content)
        result = {
            'asks': [],
            'bids': [],
        }

        for i in range(0, min(size - 1, len(orders["result"][kraken_pair]["asks"]))):
            result['asks'].append({"price": float(orders["result"][kraken_pair]["asks"][i][0]),
                                   "size": float(orders["result"][kraken_pair]["asks"][i][1])})

        for i in range(0, min(size - 1, len(orders["result"][kraken_pair]["bids"]))):
            result['bids'].append({"price": float(orders["result"][kraken_pair]["bids"][i][0]),
                                   "size": float(orders["result"][kraken_pair]["bids"][i][1])})

        return result
