import time
import pykraken.kprivate

start_time = time.time() - 60
key = 'eMVjKXkplHx6/9bNGKzl05gRlLaVi2Qu9TKX3Cw/nKmJ6odcAHKkExEP'
secret = '87rb7gZdPxMIxWJNdXUwqQms84wMyznSQVeI1nghN8yDlVp5N/9LmrTHgbrcAttYck+eEgoWO/WRcsrRKC1i+A=='
kraken_client = pykraken.Client(key=key, private_key=secret)
"""balance = pykraken.kprivate.kprivate_balance(kraken_client)
print(balance)

order = pykraken.kprivate.kprivate_addorder(kraken_client, 'XXBTZUSD', 'sell', 'limit', 7000, None, 0.01)
print(order)

balance = pykraken.kprivate.kprivate_balance(kraken_client)
print(balance)

cancel_status = pykraken.kprivate.kprivate_cancelorder(kraken_client, order['txid'][0])
print(cancel_status)

order = pykraken.kprivate.kprivate_addorder(kraken_client, 'XXBTZUSD', 'sell', 'limit', 7000, None, 0.01)
print(order)

try:
    cancel_status = pykraken.kprivate.kprivate_cancelorder(kraken_client, order['txid'][0])
    print(cancel_status)
except Exception as e:
    print(e)

trades = pykraken.kprivate.kprivate_tradeshistory(kraken_client)
print(trades)

balance = pykraken.kprivate.kprivate_balance(kraken_client)
print(balance)

closed_orders = pykraken.kprivate.kprivate_closedorders(kraken_client, True, start=start_time)
print(closed_orders)"""

#all_orders = pykraken.kprivate.kprivate_queryorders(kraken_client)
#print("All orders:", all_orders)

#cancelled_order_status = pykraken.kprivate.kprivate_queryorders(kraken_client, txid=order['txid'])
#print("Cancelled order status:", cancelled_order_status)

open_order_status = pykraken.kprivate.kprivate_queryorders(kraken_client, txid=['O6JHTU-WLJLM-OPGFC5'])
print("Open order status:", open_order_status)

#trades = pykraken.kprivate.kprivate_tradeshistory(kraken_client)
#print(trades)