import datetime


class ExecutionRateTracker:
    MAX_RATE_AGING_SEC = 60
    SIZE_FOR_PRICE_RATE_BTC = 5

    def __init__(self, max_rate_age_sec=MAX_RATE_AGING_SEC, size_for_price_rate=SIZE_FOR_PRICE_RATE_BTC):
        self._price = 0
        self._last_time = datetime.datetime.utcnow()
        self._last_size = None
        self._size_rate = 0
        self._size_for_price_rate = size_for_price_rate
        self._max_rate_age_sec = max_rate_age_sec
        self._next_size = 0

    def add_trade(self, size, price):
        try:
            prev_time = self._last_time
            self._last_time = datetime.datetime.utcnow()
            time_difference_sec = (self._last_time - prev_time).total_seconds()
            curr_size_rate = 0
            if time_difference_sec > 0:
                curr_size_rate = (size + self._next_size) / time_difference_sec
                self._next_size = 0
            else:
                self._next_size += size
            #print("Price:", price, "Size:", size, "curr size rate:", curr_size_rate, "General rate:", self._size_rate)
            if curr_size_rate > 0:
                if self._size_rate == 0 or time_difference_sec > self._max_rate_age_sec:
                    self._size_rate = curr_size_rate
                else:
                    time_ratio = time_difference_sec / self._max_rate_age_sec
                    self._size_rate = time_ratio * curr_size_rate + (1 - time_ratio) * self._size_rate
                if size > self._size_for_price_rate or self._price == 0:
                    self._price = price
                else:
                    price_ratio = size / self._size_for_price_rate
                    self._price = price_ratio * price + (1 - price_ratio) * self._price
        except Exception as e:
            print("add trade exception: ", e)

    def get_price(self):
        return self._price

    def get_size(self):
        return self._size_rate
