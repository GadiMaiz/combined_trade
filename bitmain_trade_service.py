from flask import Flask, send_from_directory, request
from bitfinex_orderbook import BitfinexOrderbook
from gdax_orderbook import GdaxOrderbook
from unified_orderbook import UnifiedOrderbook
from bitstamp_client_wrapper import BitstampClientWrapper
from bitstamp_orderbook import BitstampOrderbook
from orderbook_watchdog import OrderbookWatchdog
import logging
from logging.handlers import RotatingFileHandler
import json
import time
import sys
import getopt

app = Flask(__name__)

@app.route('/OrdersTracker')
def send_orderbook_page():
    return send_from_directory('','OrdersTracker.html')

@app.route('/GetLanguageText/<locale>')
def get_language_text(locale):
    result = {}
    with open('languages.json', encoding='utf-8') as f:
        languages = json.load(f)
        if locale in languages.keys():
            result = languages[locale]
    return str(result)

@app.route('/favicon.ico')
def send_favicon():
    return send_from_directory('','favicon.ico')

@app.route('/Orderbook/<exchange>/<currency>')
def get_orderbook(exchange, currency):
    request_orders = orderbooks[exchange]
    result = {'asks' : [], 'bids' : [], 'average_spread' : 0}
    if request_orders != None:
        if exchange == "Unified":
            curr_orders = request_orders['orderbook'].get_unified_orderbook(currency, 8)
            return str(curr_orders)
        else:
            result = request_orders['orderbook'].get_current_partial_book(request_orders['currencies_dict'][currency], 8)
            if result != None:
                result['average_spread'] = request_orders['orderbook'].get_average_spread(request_orders['currencies_dict'][currency])
                last_price = request_orders['orderbook'].get_last(currency)
                if last_price is not None:
                    result['last_price'] = last_price

    return str(result)

@app.route('/AccountBalance/<exchange>/<currency>')
def get_account_balance(exchange, currency):
    valid_currencies = ['BTC', 'BCH']
    account_balance = {}
    first_currency = True
    if currency == "ALL":
        for curr_currency in valid_currencies:
            if first_currency:
                first_currency = False
                account_balance = bitstamp_client.account_balance(curr_currency)
            else:
                curr_balance = bitstamp_client.account_balance(curr_currency)
                for curr_account_balance_key in curr_balance:
                    if curr_account_balance_key.endswith("_available") and not curr_account_balance_key.startswith("usd"):
                        account_balance[curr_account_balance_key] = curr_balance[curr_account_balance_key]
                        break
            currency_price = bitstamp_orderbook.get_current_price(curr_currency + "-USD")
            if currency_price is not None and currency_price['ask'] is not None and currency_price['bid'] is not None:
                account_balance[curr_currency.lower() + "_price"] = (currency_price['ask'] + currency_price['bid']) / 2
    elif currency in valid_currencies:
        account_balance = bitstamp_client.account_balance(currency)
        currency_price = bitstamp_orderbook.get_current_price(currency + "-USD")
        if currency_price is not None and currency_price['ask'] is not None and currency_price['bid'] is not None:
            account_balance[currency.lower() + "_price"] = (currency_price['ask'] + currency_price['bid']) / 2
    return str(account_balance)

@app.route('/BitstampTransactions')
def get_bitstamp_transactions():
    transactions_limit = None
    try:
        transactions_limit = int(request.args.get('limit'))
    except:
        transactions_limit = None
    if transactions_limit is None:
        transactions_limit = 0
    transactions = bitstamp_client.transactions(transactions_limit)
    return str(transactions)

@app.route('/IsTimedOrderRunning')
def is_time_order_running():
    result = {'time_order_running' : str(bitstamp_client.IsTimedOrderRunning())}
    return str(result)

@app.route('/GetTimedOrderStatus')
def get_timed_order_status():
    result = bitstamp_client.GetTimedOrderStatus()
    result['timed_order_running'] = str(result['timed_order_running'])
    return str(result)

@app.route('/CancelTimedOrder')
def cancel_timed_order():
    result = {'cancel_time_order_result': str(bitstamp_client.CancelTimedOrder())}
    log.info(result)
    return str(result)

@app.route('/SendOrder', methods=['POST'])
def send_order():
    log.debug("Send Order")
    start_time = time.time()
    request_params = json.loads(request.data)
    order_status = bitstamp_client.SendOrder(request_params['action_type'], float(request_params['size_coin']),
                                             request_params['crypto_type'], float(request_params['price_fiat']),
                                             request_params['fiat_type'], int(request_params['duration_sec']),
                                             float(request_params['max_order_size']))
    end_time = time.time()
    result = order_status
    result['order_status'] = str(result['order_status'])

    log.info("send command time", end_time - start_time)
    return str(result)

@app.route('/GetSentOrders', methods=['GET'])
def get_sent_orders():
    try:
        orders_limit = int(request.args.get('limit'))
    except:
        orders_limit = None

    if orders_limit is None:
        orders_limit = 0
    sent_orders = bitstamp_client.GetSentOrders(orders_limit)
    return str(sent_orders)

@app.route('/SetClientCredentials', methods=['POST'])
def set_client_credentials():
    result = {'set_credentails_status': 'True'}
    try:
        request_params = json.loads(request.data)
        bitstamp_credentials = {'username': request_params['username'],
                                'key': request_params['key'],
                                'secret': request_params['secret']}
        global bitstamp_client
        result['set_credentails_status'] = str(bitstamp_client.set_client_credentails(bitstamp_credentials))
    except:
        result['set_credentails_status'] = 'False'

    return str(result)

@app.route('/Logout')
def logout():
    result = {'set_credentials_status': 'False'}
    result['set_credentials_status'] = str(bitstamp_client.logout())
    return str(result)

@app.route('/SetBitstampCredentials', methods=['POST'])
def set_bitstamp_client_credentials():
    request_params = json.loads(request.data)
    set_credentials_result = bitstamp_client.set_client_credentails(request_params)
    result = {'set_credentials_status': str(set_credentials_result)}
    return str(result)

@app.route('/GetSignedInBitstampCredentials')
def get_bitstamp_signed_in_credentials():
    return str(bitstamp_client.get_signed_in_credentials())

@app.route('/RestartOrderbook/<exchange>')
def restart_orderbook(exchange):
    result = {"restart_result": "False",
              "exchange": exchange}
    if exchange in orderbooks:
        watchdog.restart_orderbook(exchange)
        result["restart_result"] = "True"
        """orderbooks[exchange]['orderbook'].stop_orderbook()
        if 'args' in orderbooks[exchange]:
            orderbooks[exchange]['orderbook'] = orderbooks[exchange]['creator'](orderbooks[exchange]['currencies_dict'].values())
        else:
            orderbooks[exchange]['orderbook'] = orderbooks[exchange]['creator'](
                orderbooks[exchange]['currencies_dict'].values(), **orderbooks[exchange]['args'])
        orderbooks[exchange]['orderbook'].start_orderbook()
        orderbooks['Unified']['orderbook'].set_orderbook(exchange, orderbooks[exchange]['orderbook'])"""

    return str(result)

def create_rotating_log(path):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")

    # add a rotating handler
    handler = RotatingFileHandler(path, maxBytes=20000000,
                                  backupCount=5)
    logger.addHandler(handler)

if __name__ == '__main__':

    logging.basicConfig(filename='bitmain_trade_service.log', level=logging.ERROR,
    #logging.basicConfig(level=logging.ERROR,
                        format='%(asctime)s %(processName)s %(process)d %(threadName)s %(thread)d %(levelname)s %(filename)s(%(lineno)d) %(funcName)s %(message)s')
    #handler = RotatingFileHandler('bitmain_trade_service.log', maxBytes=20000000,
    #                              backupCount=5)
    log = logging.getLogger(__name__)
    #log.addHandler(handler)
    log.error("=== Starting ===")
    argv = sys.argv[1:]
    log.info("args: %s", str(argv))
    bind_ip = None
    bitstamp_user = ''
    bitstamp_api_key = ''
    bitstamp_secret = ''
    listener_port = 5000
    verbose = False
    debug = False
    try:
        opts, args = getopt.getopt(argv, "rvu:k:s:p:")
        for opt, arg in opts:
            if opt == '-r':
                bind_ip = "0.0.0.0"
            elif opt == "-u":
                bitstamp_user = arg
            elif opt == "-k":
                bitstamp_api_key = arg
            elif opt == "-s":
                bitstamp_secret = arg
            elif opt == "-v":
                verbose = True
            elif opt == "-d":
                debug = True
            elif opt == "-p":
                try:
                    listener_port = int(arg)
                except:
                    listener_port = 5000
    except getopt.GetoptError as e:
        log.error("Parameters error:", e)

    if verbose:
        logging.basicConfig(level=logging.INFO)

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    bitstamp_credentials = None
    if bitstamp_user != '' and bitstamp_api_key != '' and bitstamp_secret != '':
        bitstamp_credentials = {'username': bitstamp_user, 'key': bitstamp_api_key, 'secret': bitstamp_secret}

    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    bitstamp_currencies = {'BTC-USD' : 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    bitstamp_args = {'log_level': logging.ERROR}
    bitstamp_orderbook = BitstampOrderbook(asset_pairs=[bitstamp_currencies['BTC-USD'], bitstamp_currencies['BCH-USD']],
                                           **bitstamp_args)
    bitstamp_orderbook.start_orderbook()

    bitfinex_currencies = {'BTC-USD': 'BTCUSD', 'BCH-USD': 'BCHUSD'}
    bitfinex_orderbook = BitfinexOrderbook([bitfinex_currencies['BTC-USD'], bitfinex_currencies['BCH-USD']])
    bitfinex_orderbook.start_orderbook()

    gdax_currencies = {'BTC-USD' : 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    gdax_orderbook = GdaxOrderbook([gdax_currencies['BTC-USD'], gdax_currencies['BCH-USD']])
    gdax_orderbook.start_orderbook()

    unified_orderbook = UnifiedOrderbook({"Bitstamp": bitstamp_orderbook,
                                          "Bitfinex": bitfinex_orderbook,
                                          "GDAX": gdax_orderbook})

    orderbooks = {'Bitstamp' : { 'orderbook' : bitstamp_orderbook, 'currencies_dict': bitstamp_currencies,
                                 'creator': BitstampOrderbook, 'args': bitstamp_args},
                  'GDAX' : { 'orderbook' : gdax_orderbook, 'currencies_dict': gdax_currencies,
                             'creator': GdaxOrderbook },
                  'Bitfinex' : { 'orderbook' : bitfinex_orderbook, 'currencies_dict': bitfinex_currencies,
                                 'creator': BitfinexOrderbook},
                  'Unified' : { 'orderbook' : unified_orderbook, 'currencies_dict': bitstamp_currencies,
                                'creator': UnifiedOrderbook}}
    watchdog = OrderbookWatchdog(orderbooks, 10)
    watchdog.start()
    bitstamp_client = BitstampClientWrapper(bitstamp_credentials, orderbooks['Bitstamp'], "./Transactions.data")
    #app.run(host= '0.0.0.0', ssl_context='adhoc')
    app.run(host=bind_ip, port=listener_port)


