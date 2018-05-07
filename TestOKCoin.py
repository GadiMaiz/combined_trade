import time
import datetime

from bitex.api.WSS import OKCoinWSS

okcoin_wss = OKCoinWSS()
okcoin_wss.start()

#time.sleep(10)
#okcoin_wss.stop()

while True:
    while not okcoin_wss.data_q.empty():
        print(time.time(),okcoin_wss.data_q.get())
    time.sleep(0.01)

"""from bitex import OKCoin
o = OKCoin()
print (o.ticker('btc_usd').content)"""