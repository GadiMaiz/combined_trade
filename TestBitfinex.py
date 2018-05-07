import time
import datetime

from bitex.api.WSS import BitfinexWSS

bitfinex_wss = BitfinexWSS()
bitfinex_wss.start()

#time.sleep(10)
#bitfinex_wss.stop()

while True:
    while not bitfinex_wss.data_q.empty():
        curr_info = bitfinex_wss.data_q.get()
        if curr_info[0] == 'order_book' and curr_info[1] == 'BTCUSD':
            print(time.time(), curr_info)
        time.sleep(0.01)