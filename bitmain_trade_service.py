from flask import Flask, send_from_directory, request
from bitfinex_orderbook import BitfinexOrderbook
from bitfinex_client_wrapper import BitfinexClientWrapper
from gdax_orderbook import GdaxOrderbook
from unified_orderbook import UnifiedOrderbook
from bitstamp_client_wrapper import BitstampClientWrapper
from bitstamp_orderbook import BitstampOrderbook
from orderbook_watchdog import OrderbookWatchdog
from exchange_clients_manager import ExchangeClientManager
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
            result = request_orders['orderbook'].get_current_partial_book(currency, 8)
            if result != None:
                result['average_spread'] = request_orders['orderbook'].get_average_spread(currency)
                last_price = request_orders['orderbook'].get_last(currency)
                if last_price is not None:
                    result['last_price'] = last_price

    return str(result)

@app.route('/AccountBalance/<exchange>')
def get_account_balance(exchange):
    account_balance = {}
    #first_currency = True
    if exchange in orderbooks:
        account_balance = exchanges_manager.exchange_balance(exchange)
    """for curr_currency in valid_currencies:
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
            account_balance[curr_currency.lower() + "_price"] = (currency_price['ask'] + currency_price['bid']) / 2"""

    return str(account_balance)

@app.route('/Transactions/<exchange>')
def get_bitstamp_transactions(exchange):
    transactions_limit = None
    try:
        transactions_limit = int(request.args.get('limit'))
    except:
        transactions_limit = None
    if transactions_limit is None:
        transactions_limit = 0
    transactions = []
    if exchange in orderbooks:
        transactions = exchanges_manager.get_exchange_transactions(exchange, transactions_limit)
    return str(transactions)

@app.route('/IsTimedOrderRunning')
def is_time_order_running():
    result = {'time_order_running': str(exchanges_manager.is_timed_order_running())}
    return str(result)

@app.route('/GetTimedOrderStatus')
def get_timed_order_status():
    result = exchanges_manager.get_timed_order_status()
    result['timed_order_running'] = str(result['timed_order_running'])
    return str(result)

@app.route('/CancelTimedOrder')
def cancel_timed_order():
    result = {'cancel_time_order_result': str(exchanges_manager.cancel_timed_order())}
    log.info(result)
    return str(result)

@app.route('/SendOrder', methods=['POST'])
def send_order():
    log.debug("Send Order")
    request_params = json.loads(request.data)
    print("Sending order in web service")
    order_status = exchanges_manager.send_order(request_params['exchanges'], request_params['action_type'],
                                                float(request_params['size_coin']), request_params['crypto_type'],
                                                float(request_params['price_fiat']), request_params['fiat_type'],
                                                int(request_params['duration_sec']),
                                                float(request_params['max_order_size']))
    result = order_status
    print(result)
    result['order_status'] = str(result['order_status'])
    log.info("command sent")
    return str(result)

@app.route('/GetSentOrders', methods=['GET'])
def get_sent_orders():
    try:
        orders_limit = int(request.args.get('limit'))
    except:
        orders_limit = None

    if orders_limit is None:
        orders_limit = 0
    sent_orders = exchanges_manager.get_sent_orders(orders_limit)
    return str(sent_orders)

@app.route('/SetClientCredentials', methods=['POST'])
def set_client_credentials():
    result = {'set_credentails_status': 'True'}
    try:
        request_params = json.loads(request.data)
        exchange = request_params['exchange']
        if exchange in orderbooks:
            credentials = {'username': request_params['username'],
                           'key': request_params['key'],
                           'secret': request_params['secret']}
        result['set_credentials_status'] = str(exchanges_manager.set_exchange_credentials(exchange, credentials))
    except:
        result['set_credentials_status'] = 'False'

    return str(result)

@app.route('/Logout/<exchange>')
def logout(exchange):
    result = {'set_credentials_status': 'False'}
    if exchange in orderbooks:
        result['set_credentials_status'] = str(exchanges_manager.logout_from_exchange(exchange))
    return str(result)

@app.route('/GetSignedInCredentials/<exchange>')
def get_bitstamp_signed_in_credentials(exchange):
    result = {'signed_in_user': "", 'is_user_signed_in': "False"}
    if exchange in orderbooks:
        result = exchanges_manager.get_signed_in_credentials(exchange)
    return str(result)

@app.route('/RestartOrderbook/<exchange>')
def restart_orderbook(exchange):
    result = {"restart_result": "False",
              "exchange": exchange}
    if exchange in orderbooks:
        watchdog.restart_orderbook(exchange)
        result["restart_result"] = "True"

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
    argv = sys.argv[1:]
    bind_ip = None
    bitstamp_user = ''
    bitstamp_api_key = ''
    bitstamp_secret = ''
    listener_port = 5000
    frozen_orderbook_timeout_sec = 20
    log_level = logging.ERROR

    try:
        opts, args = getopt.getopt(argv, "ru:k:s:p:t:l:")
        for opt, arg in opts:
            if opt == '-r':
                bind_ip = "0.0.0.0"
            elif opt == "-u":
                bitstamp_user = arg
            elif opt == "-k":
                bitstamp_api_key = arg
            elif opt == "-s":
                bitstamp_secret = arg
            elif opt == "-t":
                frozen_orderbook_timeout_sec = arg
            elif opt == "-l":
                if arg == "error":
                    log_level = logging.ERROR
                elif arg == "warning":
                    log_level = logging.WARNING
                elif arg == "debug":
                    log_level = logging.DEBUG
                elif arg == "info":
                    log_level = logging.INFO
            elif opt == "-p":
                try:
                    listener_port = int(arg)
                except:
                    listener_port = 5000
    except getopt.GetoptError as e:
        print("Parameters error:", e, "parameters:", argv)

    logging.basicConfig(filename='bitmain_trade_service.log', level=log_level,
                        format='%(asctime)s %(processName)s %(process)d %(threadName)s %(thread)d %(levelname)s %(filename)s(%(lineno)d) %(funcName)s %(message)s')
    #handler = RotatingFileHandler('bitmain_trade_service.log', maxBytes=20000000,
    #                              backupCount=5)
    log = logging.getLogger(__name__)
    #log.addHandler(handler)
    log.error("=== Starting ===")
    log.info("args: %s", str(argv))

    bitstamp_credentials = None
    if bitstamp_user != '' and bitstamp_api_key != '' and bitstamp_secret != '':
        bitstamp_credentials = {'username': bitstamp_user, 'key': bitstamp_api_key, 'secret': bitstamp_secret}

    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    print("Connecting to orderbooks")
    bitstamp_currencies = {'BTC-USD' : 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    bitstamp_args = {'log_level': logging.ERROR}
    bitstamp_orderbook = BitstampOrderbook(asset_pairs=[bitstamp_currencies['BTC-USD'], bitstamp_currencies['BCH-USD']],
                                           **bitstamp_args)
    bitstamp_orderbook.start_orderbook()
    print("Bitstamp started")
    bitfinex_currencies = {'BTC-USD': 'BTCUSD', 'BCH-USD': 'BCHUSD'}
    bitfinex_orderbook = BitfinexOrderbook([bitfinex_currencies['BTC-USD'], bitfinex_currencies['BCH-USD']])
    bitfinex_orderbook.start_orderbook()
    print("Bitfinex started")
    gdax_currencies = {'BTC-USD' : 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    gdax_orderbook = GdaxOrderbook([gdax_currencies['BTC-USD'], gdax_currencies['BCH-USD']])
    gdax_orderbook.start_orderbook()

    print("Orderbooks started")
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
    watchdog = OrderbookWatchdog(orderbooks, frozen_orderbook_timeout_sec)
    watchdog.start()
    exchanges_manager = ExchangeClientManager({'Bitstamp': {'creator': BitstampClientWrapper,
                                                            'args': {'credentials': bitstamp_credentials,
                                                                     'orderbook': orderbooks['Bitstamp']}},
                                               'Bitfinex': {'creator': BitfinexClientWrapper,
                                                            'args': {'credentials': {},
                                                                     'orderbook': orderbooks['Bitfinex']}}},
                                              "./Transactions.data",
                                              watchdog)
    #app.run(host= '0.0.0.0', ssl_context='adhoc')
    app.run(host=bind_ip, port=listener_port)
