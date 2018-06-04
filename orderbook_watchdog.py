import logging
from threading import Thread
import time

log = logging.getLogger(__name__)

class OrderbookWatchdog():
    def __init__(self, orderbooks_dict, sleep_timeout_sec=20):
        self._orderbooks_dict = orderbooks_dict
        self._watchdog_running = False
        self._watchdog_thread = None
        self._sleep_timeout_sec = sleep_timeout_sec

    def start(self):
        if self._watchdog_thread is None or not self._watchdog_thread.is_alive():
            self._watchdog_thread = Thread(target=self._monitor_orderbooks,
                                                      daemon=True,
                                                      name='Orderbooks Watchdog')
            self._watchdog_running = True
            self._watchdog_thread.start()

    def stop(self):
        self._watchdog_running = False
        if self._watchdog_thread is not None:
            self._watchdog_thread.join()

    def _monitor_orderbooks(self):
        log.info("Watchdog thread started")
        compare_orderbooks = {}
        empty_orderbook = {}
        for curr_orderbook_dict in self._orderbooks_dict:
            if self._orderbooks_dict[curr_orderbook_dict]['orderbook'].is_thread_orderbook():
                compare_orderbooks[curr_orderbook_dict] = self._get_partial_books(self._orderbooks_dict[curr_orderbook_dict])
                empty_orderbook[curr_orderbook_dict] = 0

        while self._watchdog_running:
            time.sleep(self._sleep_timeout_sec)
            current_orderbooks = {}
            for curr_orderbook_dict in self._orderbooks_dict:
                if self._orderbooks_dict[curr_orderbook_dict]['orderbook'].is_thread_orderbook():
                    current_orderbooks[curr_orderbook_dict] = self._get_partial_books(self._orderbooks_dict[curr_orderbook_dict])
                    #print("Current {}:\n{}".format(curr_orderbook_dict, current_orderbooks[curr_orderbook_dict]))
                    restarted_orderbook = False
                    log.debug("Comparing <%s>, current book is: <%s>",
                              curr_orderbook_dict, current_orderbooks[curr_orderbook_dict])
                    log.debug("Comparing <%s>, compare book is: <%s>",
                              curr_orderbook_dict, compare_orderbooks[curr_orderbook_dict])
                    if curr_orderbook_dict in compare_orderbooks and \
                            compare_orderbooks[curr_orderbook_dict] is not None and \
                            current_orderbooks[curr_orderbook_dict] is not None:
                                #print("Comparing {}\n{}".format(curr_orderbook_dict, current_orderbooks[curr_orderbook_dict]))
                                compare_result = self._compare_orderbooks(current_orderbooks[curr_orderbook_dict],
                                                                          compare_orderbooks[curr_orderbook_dict])
                                if compare_result[0] or compare_result[2]:
                                    log.error("Restarting frozen or invalid orderbook: %s, %s", curr_orderbook_dict,
                                              compare_result)
                                    empty_orderbook[curr_orderbook_dict] = 0
                                    restarted_orderbook = True
                                    self.restart_orderbook(curr_orderbook_dict)
                                elif compare_result[1]:
                                    log.error("Empty orderbook: %s", current_orderbooks[curr_orderbook_dict])
                                    empty_orderbook[curr_orderbook_dict] += 1
                                    if empty_orderbook[curr_orderbook_dict] >= 3:
                                        log.error("Restarting empty orderbook: %s\n%s", curr_orderbook_dict, empty_orderbook)
                                        empty_orderbook[curr_orderbook_dict] = 0
                                        restarted_orderbook = True
                                        self.restart_orderbook(curr_orderbook_dict)

                                else:
                                    empty_orderbook[curr_orderbook_dict] = 0
                    if not restarted_orderbook:
                        compare_orderbooks[curr_orderbook_dict] = current_orderbooks[curr_orderbook_dict]
                        log.debug("Not restarting %s", curr_orderbook_dict)
                    else:
                        compare_orderbooks[curr_orderbook_dict] = None
                        log.debug("Restarting %s", curr_orderbook_dict)

                    if restarted_orderbook:
                        compare_orderbooks[curr_orderbook_dict] = None

    @staticmethod
    def _get_partial_books(orderbook_dict):
        result = {}
        for currency in orderbook_dict['currencies_dict']:
            result[currency] = orderbook_dict['orderbook'].get_current_partial_book(orderbook_dict['currencies_dict'][currency], 8)

        return result

    @staticmethod
    def _compare_orderbooks(curr_orderbook, prev_orderbook):
        identical_books = False
        empty_orderbook = False
        invalid_workbook = False
        for currency in prev_orderbook:
            if len(curr_orderbook[currency]['asks']) == 0 or len(curr_orderbook[currency]['bids']) == 0:
                empty_orderbook = True
                #print("Empty orderbook for {}: {} asks, {} bids".format(currency, len(curr_orderbook[currency]['asks']), len(curr_orderbook[currency]['bids'])))
                #print(curr_orderbook[currency])
            elif curr_orderbook[currency]['asks'][0]['price'] < curr_orderbook[currency]['bids'][0]['price']:
                invalid_workbook = True
                break
            else:
                identical_books = OrderbookWatchdog._compare_sub_orderbooks(prev_orderbook[currency]['asks'],
                                                                            curr_orderbook[currency]['asks']) or \
                                  OrderbookWatchdog._compare_sub_orderbooks(prev_orderbook[currency]['bids'],
                                                                            curr_orderbook[currency]['bids'])
            if identical_books:
                break
        return [identical_books, empty_orderbook, invalid_workbook]

    @staticmethod
    def _compare_sub_orderbooks(prev_sub_orderbook, curr_sub_orderbook):

        result = True
        if len(prev_sub_orderbook) != len(curr_sub_orderbook) or len(curr_sub_orderbook) < 5:
            result = False
        else:
            order_index = 0
            while order_index < len(prev_sub_orderbook) and result:
                if prev_sub_orderbook[order_index]['price'] != \
                        curr_sub_orderbook[order_index]['price'] or \
                        prev_sub_orderbook[order_index]['size'] != \
                        curr_sub_orderbook[order_index]['size']:
                    #print("different orderbook:", order_index, prev_sub_orderbook, curr_sub_orderbook)
                    result = False
                order_index += 1

        return result

    def restart_orderbook(self, exchange):
        self._orderbooks_dict[exchange]['orderbook'].stop_orderbook()
        if 'args' not in self._orderbooks_dict[exchange]:
            self._orderbooks_dict[exchange]['orderbook'] = self._orderbooks_dict[exchange]['creator'](self._orderbooks_dict[exchange]['currencies_dict'].values())
        else:
            self._orderbooks_dict[exchange]['orderbook'] = self._orderbooks_dict[exchange]['creator'](
                self._orderbooks_dict[exchange]['currencies_dict'].values(), **self._orderbooks_dict[exchange]['args'])
        self._orderbooks_dict[exchange]['orderbook'].start_orderbook()
        self._orderbooks_dict['Unified']['orderbook'].set_orderbook(exchange, self._orderbooks_dict[exchange]['orderbook'])