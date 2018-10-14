import logging
from threading import Thread
import time
from orderbook_base import OrderbookFee
import psutil
import os
import threading

log = logging.getLogger(__name__)


class OrderbookWatchdog():
    def __init__(self, orderbooks_dict, sleep_timeout_sec=20):
        self._orderbooks_dict = orderbooks_dict
        self._watchdog_running = False
        self._watchdog_thread = None
        self._sleep_timeout_sec = sleep_timeout_sec
        self._running_orderbooks = dict()

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
        log.debug("Watchdog thread started")
        compare_orderbooks = {}
        empty_orderbook = {}
        for curr_orderbook_dict in self._orderbooks_dict:
            if self._orderbooks_dict[curr_orderbook_dict]['orderbook'] and \
                    self._orderbooks_dict[curr_orderbook_dict]['orderbook'].is_thread_orderbook():
                compare_orderbooks[curr_orderbook_dict] = self._get_partial_books(self._orderbooks_dict[curr_orderbook_dict])
                empty_orderbook[curr_orderbook_dict] = 0
        #restart_counter = 1
        restarts_log = []
        while self._watchdog_running:
            #restart_counter += 1
            time.sleep(self._sleep_timeout_sec)
            process = psutil.Process(os.getpid())
            threads = threading.enumerate()
            """print(time.time(), "Memory usage:", process.memory_info().rss, "Number of threads:", len(threads),
                  "Restarts:", len(restarts_log), restarts_log)
            for curr_thread in threads:
                print(curr_thread)"""
            current_orderbooks = {}

            for curr_orderbook_dict in self._orderbooks_dict:
                if self._orderbooks_dict[curr_orderbook_dict]['orderbook'] and \
                        self._orderbooks_dict[curr_orderbook_dict]['orderbook'].is_thread_orderbook():
                    current_orderbooks[curr_orderbook_dict] = \
                        self._get_partial_books(self._orderbooks_dict[curr_orderbook_dict])
                    #print("Current {}:\n{}".format(curr_orderbook_dict, current_orderbooks[curr_orderbook_dict]))
                    restarted_orderbook = False
                    log.debug("Comparing <%s>, current book is: <%s>",
                              curr_orderbook_dict, current_orderbooks[curr_orderbook_dict])
                    log.debug("Comparing <%s>, compare book is: <%s>",
                              curr_orderbook_dict, compare_orderbooks[curr_orderbook_dict])
                    if curr_orderbook_dict in compare_orderbooks and \
                            compare_orderbooks[curr_orderbook_dict] and \
                            current_orderbooks[curr_orderbook_dict]:
                                #print("Comparing {}\n{}".format(curr_orderbook_dict, current_orderbooks[curr_orderbook_dict]))
                                compare_result = self._compare_orderbooks(current_orderbooks[curr_orderbook_dict],
                                                                          compare_orderbooks[curr_orderbook_dict])
                                if compare_result[0] or compare_result[2]:
                                    log.error("Restarting frozen or invalid orderbook: %s, %s", curr_orderbook_dict,
                                              compare_result)
                                    empty_orderbook[curr_orderbook_dict] = 0
                                    restarted_orderbook = True
                                    self.restart_orderbook(curr_orderbook_dict)
                                    restarts_log.append(curr_orderbook_dict)
                                    #number_of_restarts += 1
                                elif compare_result[1]: #or restart_counter % 30 == 0:
                                    #if restart_counter % 30 == 0:
                                        #restart_counter = 0
                                        #print("Restarting after some attemps")
                                    log.error("Empty orderbook: %s", current_orderbooks[curr_orderbook_dict])
                                    empty_orderbook[curr_orderbook_dict] += 1
                                    if empty_orderbook[curr_orderbook_dict] >= 3:
                                        log.error("Restarting empty orderbook: %s\n%s", curr_orderbook_dict, empty_orderbook)
                                        empty_orderbook[curr_orderbook_dict] = 0
                                        restarted_orderbook = True
                                        self.restart_orderbook(curr_orderbook_dict)
                                        #number_of_restarts += 1
                                        restarts_log.append(curr_orderbook_dict)
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
            result[currency] = \
                orderbook_dict['orderbook'].get_current_partial_book(orderbook_dict['currencies_dict'][currency], 8,
                                                                     OrderbookFee.NO_FEE)

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
        #return [identical_books and False, empty_orderbook and False, invalid_workbook and False]
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
            #self.stop_orderbook(exchange)
            #self.start_orderbook(exchange)
            pass

    def stop_orderbook(self, exchange):
        if self._orderbooks_dict[exchange]['active']:
            self._orderbooks_dict[exchange]['orderbook'].stop_orderbook()
            self._orderbooks_dict['Unified']['orderbook'].set_orderbook(exchange, None)
            self._orderbooks_dict[exchange]['orderbook'] = None
            self._orderbooks_dict[exchange]['active'] = False
            for identifier in self._running_orderbooks:
                self._running_orderbooks[identifier].set_orderbook(exchange, None)

    def start_orderbook(self, exchange):
        if not self._orderbooks_dict[exchange]['active']:
            if 'args' not in self._orderbooks_dict[exchange]:
                self._orderbooks_dict[exchange]['orderbook'] = \
                    self._orderbooks_dict[exchange]['creator'](
                        self._orderbooks_dict[exchange]['currencies_dict'].values(),
                        self._orderbooks_dict[exchange]['fees'])
            else:
                self._orderbooks_dict[exchange]['orderbook'] = \
                    self._orderbooks_dict[exchange]['creator'](
                        self._orderbooks_dict[exchange]['currencies_dict'].values(),
                        self._orderbooks_dict[exchange]['fees'],
                        **self._orderbooks_dict[exchange]['args'])
            self._orderbooks_dict[exchange]['orderbook'].start_orderbook()
            self._orderbooks_dict['Unified']['orderbook'].set_orderbook(exchange,
                                                                        self._orderbooks_dict[exchange]['orderbook'])
            self._orderbooks_dict[exchange]['active'] = True
            for identifier in self._running_orderbooks:
                self._running_orderbooks[identifier].set_orderbook(exchange,
                                                                   self._orderbooks_dict[exchange]['orderbook'])

    def register_orderbook(self, identifier, orderbook):
        self._running_orderbooks[identifier] = orderbook

    def unregister_orderbook(self, identifier):
        self._running_orderbooks.pop(identifier, None)

    def get_active_exchanges(self):
        active_orderbooks = []
        for exchange in self._orderbooks_dict:
            if self._orderbooks_dict[exchange]['active']:
                active_orderbooks.append(exchange)
        return active_orderbooks
