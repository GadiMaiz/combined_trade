import bitstamp.client
trading_client = bitstamp.client.Trading(username='934248', key='pCYdWylg6SOYraz4yv1MVltIYJsrMrbD', secret='2tN0zNUX9fmHjzPhxKDkvTtDKaBWcNNU')

#import trade_client
#trading_client = trade_client.TradeClient()
print('balance',trading_client.account_balance())
sent_order = trading_client.sell_limit_order(0.0008, 10000)
#sent_order = trading_client.buy_limit_order(0.0018, 7962)
print('sent order', sent_order,'balance',trading_client.account_balance())
all_orders = trading_client.open_orders()
for curr_order in all_orders:
    trading_client.cancel_order(curr_order.get("id"))
print(len(all_orders), "open orders: ", all_orders)
order_status = trading_client.order_status(sent_order.get("id"))
print("order status before cancel", order_status)
result = trading_client.cancel_order(sent_order.get("id"))
order_status = trading_client.order_status(sent_order.get("id"))
print(result, trading_client.account_balance())
#order_status = trading_client.order_status(sent_order.get("id"))
#print("order status after cancel", order_status)
all_orders = trading_client.open_orders()
print(len(all_orders), "open orders: ", all_orders)
trade_history = trading_client.user_transactions()
print('transactions', trade_history)

