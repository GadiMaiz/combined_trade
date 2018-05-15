import gdax
from threading import Thread
import asyncio
from orderbook_base import OrderbookBase
import logging

class GdaxOrderbook(OrderbookBase):
    def __init__(self, asset_pairs):
        super().__init__(asset_pairs)
        self.running = False
        self._orderbook_thread = None
        self._orderbook = None
        self._init_complete = False
        self._log = logging.getLogger(__name__)
    def _start(self):
        if self._orderbook_thread is None or not self._orderbook_thread.is_alive():
            self._orderbook_thread = Thread(target=self._manage_orderbook,
                                             daemon=True,
                                             name='Manage Orderbook Thread')
            self.running = True
            self._event_loop = asyncio.get_event_loop()
            self._orderbook_thread.start()

    def _stop(self):
        """
        Stops Threads. Overwrite this in your child class as necessary.
        :return:
        """
        self.running = False
        self._orderbook_thread.join()

    def _manage_orderbook(self):
        self._event_loop.run_until_complete(self._manage_orderbook_async())

    async def _manage_orderbook_async(self):
        self._init_complete = False
        async with gdax.orderbook.OrderBook(['BTC-USD', 'BCH-USD']) as self._orderbook:
            self._init_complete = True
            while self.running:
                try:
                    message = await self._orderbook.handle_message()
                except Exception as e:
                    self._log.error("Error handling message: <%s>, error is: <%s>", str(message), str(e))

    def get_current_partial_book(self, product_id, book_size):
        result = {
            'asks': [],
            'bids': [],
        }

        if self._init_complete:
            index = 0
            asks = self._orderbook.get_all_asks(product_id)
            curr_ask_price, ask = asks.min_item()
            while (index < book_size and index < len(asks)):
                try:
                    # There can be a race condition here, where a price point is
                    # removed between these two ops
                    this_ask = asks[curr_ask_price]
                except KeyError:
                    continue
                if len(this_ask) > 0:
                    this_sub_ask = {'price': float(this_ask[0]['price']),
                                    'size': float(sum([order['size'] for order in this_ask])),
                                    'source': 'GDAX'}
                    result['asks'].append(this_sub_ask)
                index += 1
                if index < len(asks):
                    curr_ask_price, ask = asks.succ_item(curr_ask_price)

            index = 0
            bids = self._orderbook.get_all_bids(product_id)
            curr_bid_price, bid = bids.max_item()
            while (index < book_size and index < len(bids)):
                try:
                    # There can be a race condition here, where a price point is
                    # removed between these two ops
                    this_bid = bids[curr_bid_price]
                except KeyError:
                    continue
                if len(this_bid) > 0:
                    this_sub_bid = {'price': float(this_bid[0]['price']),
                                    'size': float(sum([order['size'] for order in this_bid])),
                                    'source' : 'GDAX'}
                    result['bids'].append(this_sub_bid)
                index += 1
                if index < len(bids):
                    curr_bid_price, bid = bids.prev_item(curr_bid_price)

        return result