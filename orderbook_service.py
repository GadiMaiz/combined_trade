import asyncio
import gdax
from aiohttp import web
from bitex.api.WSS.bitstamp import BitstampWSS
#from bitex.api.WSS.okcoin import OKCoinWSS
import kraken_orderbook
from bitfinex_orderbook import BitfinexOrderbook
from unified_orderbook import UnifiedOrderbook

curr_gdax_orderbook = []
curr_kraken_orderbook = []

async def run_gdax_orderbook():
    print ("Starting GDAX")
    global curr_gdax_orderbook
    async with gdax.orderbook.OrderBook(['BTC-USD']) as orderbook:
        curr_gdax_orderbook = orderbook
        print ("GDAX Orderbook initialized")
        while True:
            message = await curr_gdax_orderbook.handle_message()

async def run_kraken_orderbook():
    global curr_kraken_orderbook
    kraken_query_client = kraken_orderbook.KrakenQuery()
    curr_kraken_orderbook = kraken_query_client.get_current_partial_book('BTC-USD', 10)
    print("Kraken Orderbook initialized")
    while True:
        await asyncio.sleep(2)
        curr_kraken_orderbook = kraken_query_client.get_current_partial_book('BTC-USD', 10)

async def hello(request):
    return web.Response(text="Hello World!")
   
async def get_gdax_orders(request):
    curr_orders = []
    if str(type(curr_gdax_orderbook)) == '<class \'gdax.orderbook.OrderBook\'>':
        curr_orders = curr_gdax_orderbook.get_current_partial_book('BTC-USD', 10)

    return web.Response(text=str(curr_orders))

async def get_kraken_orders(request):
    return web.Response(text=str(curr_kraken_orderbook))
    
async def get_bitstamp_orders(request):
    curr_orders = bitstamp_wss.get_current_partial_book('BTC-USD', 10)
    return web.Response(text=str(curr_orders))

async def get_okcoin_orders(request):
    curr_orders = okcoin_wss.get_current_partial_book('btc_usd', 10)
    return web.Response(text=str(curr_orders))

async def get_bitfinex_orders(request):
    curr_orders = bitfinex_orderbook.get_current_partial_book('btc_usd', 10)
    return web.Response(text=str(curr_orders))

async def get_unified_orderbook(request):
    global unified_orderbook
    curr_orders = unified_orderbook.get_unified_orderbook('BTC-USD', 10)
    return web.Response(text=str(curr_orders))

async def OrdersTracker(request):
    return web.FileResponse('./OrdersTracker.html')
 
if __name__ == '__main__':
    app = web.Application()
    app.router.add_get('/hello', hello)
    app.router.add_get('/GDAX', get_gdax_orders)
    app.router.add_get('/Bitstamp', get_bitstamp_orders)
    app.router.add_get('/Kraken', get_kraken_orders)
    app.router.add_get('/Bitfinex', get_bitfinex_orders)
    #app.router.add_get('/OKCoin', get_okcoin_orders)
    app.router.add_get('/OrdersTracker', OrdersTracker)
    app.router.add_get('/UnifiedOrderbook', get_unified_orderbook)
    
    bitstamp_wss = BitstampWSS()
    bitstamp_wss.start()

    bitfinex_orderbook = BitfinexOrderbook()
    bitfinex_orderbook.start()

    unified_orderbook = UnifiedOrderbook([bitstamp_wss, bitfinex_orderbook])
    
    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler, '0.0.0.0', 8080)
    #loop.run_until_complete(asyncio.gather(run_kraken_orderbook(), run_gdax_orderbook(), f))
    #loop.run_until_complete(f)
    web.run_app(app)