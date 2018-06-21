from flask import Flask, send_from_directory, request
from bitfinex_orderbook import BitfinexOrderbook
from orderbook_base import OrderbookFee
from bitfinex_client_wrapper import BitfinexClientWrapper
from gdax_orderbook import GdaxOrderbook
from unified_orderbook import UnifiedOrderbook
from bitstamp_client_wrapper import BitstampClientWrapper
from bitstamp_orderbook import BitstampOrderbook
from kraken_orderbook import KrakenOrderbook
from kraken_client_wrapper import KrakenClientWrapper
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
def get_orderbook_str(exchange, currency):
    return str(get_orderbook(exchange, currency))

def get_orderbook(exchange, currency):
    result = {'asks': [], 'bids': [], 'average_spread': 0, 'currency': currency}
    if exchange in orderbooks and orderbooks[exchange]:
        request_orders = orderbooks[exchange]
        if exchange == "Unified":
            curr_orders = request_orders['orderbook'].get_unified_orderbook(currency, 8, OrderbookFee.NO_FEE)
            curr_orders['currency'] = currency
            return curr_orders
        else:
            if request_orders['orderbook']:
                result = request_orders['orderbook'].get_current_partial_book(currency, 8, OrderbookFee.NO_FEE)
                result['currency'] = currency
                if result != None:
                    result['average_spread'] = request_orders['orderbook'].get_average_spread(currency)
                    last_price = request_orders['orderbook'].get_last(currency)
                    if last_price is not None:
                        result['last_price'] = last_price
                    result['rate'] = request_orders['orderbook'].get_tracked_info(currency)

    return result


@app.route('/AccountBalance')
def get_all_accounts_balance():
    account_balances = exchanges_manager.get_all_account_balances()
    return str(account_balances)


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
        fees = dict()
        try:
            fees['take'] = float(request_params['taker_fee'])
        except ValueError as e:
            pass
        try:
            fees['make'] = float(request_params['maker_fee'])
        except ValueError as e:
            pass
        if exchange in orderbooks and 'username' in request_params and 'key' in request_params and \
                'secret' in request_params:
            credentials = {'username': request_params['username'],
                           'key': request_params['key'],
                           'secret': request_params['secret']}
            orderbooks[exchange]['fees'].update(fees)
            orderbooks[exchange]['orderbook'].set_fees(orderbooks[exchange]['fees'])
        result['set_credentials_status'] = str(exchanges_manager.set_exchange_credentials(exchange, credentials))
    except Exception as e:
        result['set_credentials_status'] = 'False'

    return str(result)


@app.route('/Logout/<exchange>')
def logout(exchange):
    result = {'set_credentials_status': 'False'}
    if exchange in orderbooks:
        result['set_credentials_status'] = str(exchanges_manager.logout_from_exchange(exchange))
    return str(result)


@app.route('/GetSignedInCredentials')
def get_signed_in_credentials():
    result = {}
    for exchange in orderbooks:
        result[exchange] = exchanges_manager.get_signed_in_credentials(exchange)
    return str(result)


@app.route('/RestartOrderbook/<exchange>')
def restart_orderbook(exchange):
    result = {"restart_result": "False",
              "exchange": exchange}
    if exchange in orderbooks:
        watchdog.restart_orderbook(exchange)
        result["restart_result"] = "True"

    return str(result)

@app.route('/StartOrderbook/<exchange>')
def start_orderbook(exchange):
    result = {"start_result": "False",
              "exchange": exchange}
    if exchange in orderbooks:
        watchdog.start_orderbook(exchange)
        result["start_result"] = "True"
    return str(result)

@app.route('/StopOrderbook/<exchange>')
def stop_orderbook(exchange):
    result = {"stop_result": "False",
              "exchange": exchange}
    if exchange in orderbooks:
        watchdog.stop_orderbook(exchange)
        result["stop_result"] = "True"
        logout(exchange)
    return str(result)

@app.route('/ActiveExchanges')
def get_active_exchanges():
    return str(watchdog.get_active_exchanges())

@app.route('/ActiveOrderbooks/<currency>')
def get_active_orderbooks(currency):
    active_exchanges = watchdog.get_active_exchanges()
    active_exchanges.append("Unified")
    result = dict()
    for exchange in active_exchanges:
        result[exchange] = get_orderbook(exchange, currency)

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
    frozen_orderbook_timeout_sec = 30
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
    bitstamp_currencies = {'BTC-USD': 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    bitstamp_args = {'log_level': logging.ERROR}
    bitstamp_fees = {'take': 0.25, 'make': 0.25}
    bitstamp_orderbook = BitstampOrderbook(asset_pairs=[bitstamp_currencies['BTC-USD'], bitstamp_currencies['BCH-USD']],
                                           fees=bitstamp_fees, **bitstamp_args)
    bitstamp_orderbook.start_orderbook()
    print("Bitstamp started")
    bitfinex_currencies = {'BTC-USD': 'BTCUSD', 'BCH-USD': 'BCHUSD'}
    bitfinex_fees = {'take': 0.1, 'make': 0.2}
    bitfinex_orderbook = BitfinexOrderbook([bitfinex_currencies['BTC-USD'], bitfinex_currencies['BCH-USD']],
                                           bitfinex_fees)
    bitfinex_orderbook.start_orderbook()
    print("Bitfinex started")
    gdax_currencies = {'BTC-USD' : 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    gdax_fees = {'take': 0.3, 'make': 0}
    gdax_orderbook = GdaxOrderbook([gdax_currencies['BTC-USD'], gdax_currencies['BCH-USD']], gdax_fees)
    gdax_orderbook.start_orderbook()

    kraken_fees = {'take': 0.26, 'make': 0.16}
    kraken_orderbook = KrakenOrderbook(['BTC-USD', 'BCH-USD'], kraken_fees)
    kraken_orderbook.start_orderbook()

    print("Orderbooks started")
    unified_orderbook = UnifiedOrderbook({"Bitstamp": bitstamp_orderbook,
                                          "Bitfinex": bitfinex_orderbook,
                                          "GDAX": gdax_orderbook,
                                          "Kraken": kraken_orderbook})

    orderbooks = {'Bitstamp': {'orderbook': bitstamp_orderbook, 'currencies_dict': bitstamp_currencies,
                               'creator': BitstampOrderbook, 'args': bitstamp_args, 'active': True,
                               'fees': bitstamp_fees},
                  'GDAX': {'orderbook': gdax_orderbook, 'currencies_dict': gdax_currencies,
                           'creator': GdaxOrderbook, 'active': True, 'fees': gdax_fees},
                  'Bitfinex': {'orderbook': bitfinex_orderbook, 'currencies_dict': bitfinex_currencies,
                               'creator': BitfinexOrderbook, 'active': True, 'fees': bitfinex_fees},
                  'Kraken': {'orderbook': kraken_orderbook, 'currencies_dict': bitstamp_currencies,
                             'creator': KrakenOrderbook, 'active': True, 'fees': kraken_fees},
                  'Unified': {'orderbook': unified_orderbook, 'currencies_dict': bitstamp_currencies,
                              'creator': UnifiedOrderbook, 'active': False, 'fees': dict()}}
    watchdog = OrderbookWatchdog(orderbooks, frozen_orderbook_timeout_sec)
    watchdog.start()
    exchanges_manager = ExchangeClientManager({'Bitstamp': {'creator': BitstampClientWrapper,
                                                            'args': {'credentials': bitstamp_credentials,
                                                                     'orderbook': orderbooks['Bitstamp']}},
                                               'Bitfinex': {'creator': BitfinexClientWrapper,
                                                            'args': {'credentials': {},
                                                                     'orderbook': orderbooks['Bitfinex']}},
                                               'Kraken': {'creator': KrakenClientWrapper,
                                                          'args': {'credentials': {},
                                                                   'orderbook': orderbooks['Kraken']}}
                                               },
                                              "./Transactions.data",
                                              watchdog)
    #app.run(host= '0.0.0.0', ssl_context='adhoc')
    app.run(host=bind_ip, port=listener_port)
