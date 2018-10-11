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
from huobi_orderbook import HuobiOrderbook
from orderbook_watchdog import OrderbookWatchdog
from exchange_clients_manager import ExchangeClientManager
import logging
from logging.handlers import RotatingFileHandler
import json
import re
import sys
import getopt
import time
import os
import json

app = Flask(__name__)


client_dir = os.path.join(app.root_path, 'client')

@app.route('/OrdersTracker')
def send_orderbook_page():
    return send_from_directory(client_dir, 'OrdersTracker.html')


@app.route('/GetLanguageText/<locale>')
def get_language_text(locale):
    print(str(time.time()) + " start get_language_text")
    result = {}
    with open('languages.json', encoding='utf-8') as f:
        languages = json.load(f)
        if locale in languages.keys():
            result = languages[locale]
    print(str(time.time()) + " end get_language_text")
    return str(result)


@app.route('/favicon.ico')
def send_favicon():
    return send_from_directory(client_dir, 'favicon.ico')


@app.route('/bundle.js')
def send_bundle():
    return send_from_directory(client_dir, 'bundle.js')


@app.route('/Orderbook/<exchange>/<currency>')
def get_orderbook_str(exchange, currency):
    #print(str(time.time()) + " start get_orderbook_str")
    result = str(get_orderbook(exchange, currency))
    #print(str(time.time()) + " end get_orderbook_str")
    return result


def get_orderbook(exchange, currency):
    #print(str(time.time()) + " start get_orderbook")
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

    #print(str(time.time()) + " end get_orderbook")
    return result


@app.route('/AccountBalance')
def get_all_accounts_balance():
    account_balances = exchanges_manager.get_all_account_balances(False)
    return str(account_balances)


@app.route('/AccountBalanceForce')
def get_all_accounts_balance_force():
    account_balances = exchanges_manager.get_all_account_balances(True)
    return str(account_balances)


@app.route('/Transactions/<exchange>')
def get_bitstamp_transactions(exchange):
    #print(str(time.time()) + " start get_bitstamp_transactions")
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
    #print(str(time.time()) + " end get_bitstamp_transactions")
    return str(transactions)

@app.route('/IsTimedOrderRunning')
def is_time_order_running():
    result = {'time_order_running': str(exchanges_manager.is_timed_order_running())}
    return str(result)


@app.route('/GetTimedOrderStatus')
def get_timed_order_status():
    #print(str(time.time()) + " start get_timed_order_status")
    result = exchanges_manager.get_timed_order_status()
    result['timed_order_running'] = str(result['timed_order_running'])
    #print(str(time.time()) + " end get_timed_order_status")
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
    #print("Sending order in web service")
    result = dict()
    result['order_status'] = str('Invalid parameters')
    if request_params['fiat_type'] in ['USD'] and request_params['crypto_type'] in ['BTC', 'BCH']:
        order_status = exchanges_manager.send_order(request_params['exchanges'], request_params['action_type'],
                                                    float(request_params['size_coin']), request_params['crypto_type'],
                                                    float(request_params['price_fiat']), request_params['fiat_type'],
                                                    int(request_params['duration_sec']),
                                                    float(request_params['max_order_size']))
        result = order_status
    #print(result)
    result['order_status'] = str(result['order_status'])
    log.info("command sent")
    return str(result)


@app.route('/GetSentOrders', methods=['GET'])
def get_sent_orders():
    #print(str(time.time()) + " start get_sent_orders")
    try:
        orders_limit = int(request.args.get('limit'))
    except:
        orders_limit = None

    if orders_limit is None:
        orders_limit = 0
    sent_orders = exchanges_manager.get_sent_orders(orders_limit)
    #print(str(time.time()) + " end get_sent_orders")
    return str(sent_orders)


@app.route('/GetSentOrdersFiltered', methods=['POST'])
def get_sent_orders_filtered():
    #print(str(time.time()) + " start get_sent_orders_filtered")
    sent_orders = []
    request_filter = {}
    orders_limit = 0
    valid_parameters = False
    try:
        request_params = json.loads(request.data)
        request_filter = request_params['filter']
        orders_limit = int(request_params['limit'])
        valid_parameters = True
    except Exception as ex:
        print("GetSentOrdersFiltered parameters error: {}".format(ex))

    if valid_parameters:
        sent_orders = exchanges_manager.get_sent_orders(orders_limit, request_filter)
    #print(str(time.time()) + " start get_sent_orders_filtered")
    return str(sent_orders)


@app.route('/SetClientCredentials', methods=['POST'])
def set_client_credentials():
    result = {'set_credentials_status': 'True'}
    try:
        request_params = json.loads(request.data)
        exchange = request_params['exchange']
        fees = dict()
        try:
            fee = float(request_params['taker_fee'])
            if 0 <= fee < 100:
                fees['take'] = fee
        except ValueError as e:
            pass
        try:
            fee = float(request_params['maker_fee'])
            if 0 <= fee < 100:
                fees['make'] = fee
        except ValueError as e:
            pass
        if exchange in orderbooks and 'username' in request_params and 'key' in request_params and \
                'secret' in request_params:
            # Make sure that the username is a number
            user_reg = re.compile('^[0-9\.]+$')
            key_reg = re.compile('^[0-9a-zA-Z=+\./]+$')
            if user_reg.match(request_params['username']) and key_reg.match(request_params['key']) and \
                    key_reg.match(request_params['secret']):
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
    #print(str(time.time()) + " start get_signed_in_credentials")
    result = {}
    for exchange in orderbooks:
        result[exchange] = exchanges_manager.get_signed_in_credentials(exchange)
    #print(str(time.time()) + " end get_signed_in_credentials")
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
    #print(str(time.time()) + " start get_active_orderbooks")
    active_exchanges = watchdog.get_active_exchanges()
    active_exchanges.append("Unified")
    result = dict()
    for exchange in active_exchanges:
        result[exchange] = get_orderbook(exchange, currency)

    #print(str(time.time()) + " end get_active_orderbooks")
    return str(result)

def create_rotating_log(log_file, log_level):
    logging.basicConfig(filename=log_file, level=log_level,
                        format='%(asctime)s %(levelname)s %(message)s %(filename)s(%(lineno)d) %(funcName)s %(threadName)s %(thread)d')
    logger = logging.getLogger(__name__)
    
    # add a rotating handler
    handler = RotatingFileHandler(log_file, maxBytes=20000000, backupCount=5)
    logger.addHandler(handler)

if __name__ == '__main__':
    argv = sys.argv[1:]
    bind_ip = None
    bitstamp_user = ''
    bitstamp_api_key = ''
    bitstamp_secret = ''
    listener_port = 5000
    frozen_orderbook_timeout_sec = 60
    log_level = logging.ERROR
    bitstamp_key = None
    start_exchanges = ['Bitstamp', 'Bitfinex', 'GDAX', 'Kraken', 'Huobi']
    open_log = True
    if 'EXCHANGES_CREDENTIALS' in os.environ:
        exchanges_credentials = os.environ['EXCHANGES_CREDENTIALS']

    try:
        opts, args = getopt.getopt(argv, "rdc:p:t:l:b:e:")
        for opt, arg in opts:
            if opt == '-r':
                bitstamp_key = opt
            elif opt == '-d':
                bind_ip = "0.0.0.0"
            elif opt == '-c':
                exchanges_credentials = arg
            # elif opt == "-u":
            #     bitstamp_user = arg
            # elif opt == "-k":
            #     bitstamp_api_key = arg
            # elif opt == "-s":
            #     bitstamp_secret = arg
            elif opt == "-t":
                frozen_orderbook_timeout_sec = arg
            elif opt == "-e":
                start_exchanges = arg.split("/")
            elif opt == "-l":
                if arg == "error":
                    log_level = logging.ERROR
                elif arg == "warning":
                    log_level = logging.WARNING
                elif arg == "debug":
                    log_level = logging.DEBUG
                elif arg == "info":
                    log_level = logging.INFO
                elif arg == "none":
                    open_log = False
            elif opt == "-p":
                try:
                    listener_port = int(arg)
                except:
                    listener_port = 5000
    except getopt.GetoptError as e:
        print("Parameters error:", e, "parameters:", argv)

    if open_log:
        log_file = os.path.join(app.root_path, 'logs', 'bitmain_trade_service.log')
        create_rotating_log(log_file, log_level)
    
    # print('exchanges_credentials: ', exchanges_credentials)
    exchanges_credentials = json.loads(exchanges_credentials)
    # print('exchanges_credentials (JSON): ', exchanges_credentials)

    log = logging.getLogger(__name__)
    #log.addHandler(handler)
    log.info("=== Starting ===")
    log.debug("args: %s", str(argv))

    bitstamp_credentials = None
    if 'Bitstamp' in exchanges_credentials:
        bitstamp_credentials = exchanges_credentials['Bitstamp']
        # if bitstamp_user != '' and bitstamp_api_key != '' and bitstamp_secret != '':
        #     bitstamp_credentials = {'username': bitstamp_user, 'key': bitstamp_api_key, 'secret': bitstamp_secret}

    # log = logging.getLogger('werkzeug')
    # log.setLevel(log_level)

    print("Connecting to orderbooks")
    bitstamp_currencies = {'BTC-USD': 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    bitstamp_inner_logger = logging.ERROR
    if log_level is logging.DEBUG:
        bitstamp_inner_logger = logging.DEBUG
    bitstamp_args = {'log_level': bitstamp_inner_logger}
    if bitstamp_key is not None:
        bitstamp_args['key'] = bitstamp_key
    bitstamp_fees = {'take': 0.25, 'make': 0.25}
    bitstamp_orderbook = BitstampOrderbook(asset_pairs=[bitstamp_currencies['BTC-USD'], bitstamp_currencies['BCH-USD']],
                                           fees=bitstamp_fees, **bitstamp_args)
    active_exchanges = dict()
    active_exchanges['Bitstamp'] = False
    if "Bitstamp" in start_exchanges:
        bitstamp_orderbook.start_orderbook()
        active_exchanges['Bitstamp'] = True
        print("Bitstamp started")

    bitfinex_currencies = {'BTC-USD': 'BTCUSD', 'BCH-USD': 'BCHUSD'}
    bitfinex_fees = {'take': 0.1, 'make': 0.2}
    bitfinex_orderbook = BitfinexOrderbook([bitfinex_currencies['BTC-USD'], bitfinex_currencies['BCH-USD']],
                                           bitfinex_fees)
    active_exchanges['Bitfinex'] = False
    if "Bitfinex" in start_exchanges:
        bitfinex_orderbook.start_orderbook()
        active_exchanges['Bitfinex'] = True
        print("Bitfinex started")

    gdax_currencies = {'BTC-USD': 'BTC-USD', 'BCH-USD': 'BCH-USD'}
    gdax_fees = {'take': 0.3, 'make': 0}
    gdax_orderbook = GdaxOrderbook([gdax_currencies['BTC-USD'], gdax_currencies['BCH-USD']], gdax_fees)

    active_exchanges['GDAX'] = False
    if "GDAX" in start_exchanges:
        gdax_orderbook.start_orderbook()
        active_exchanges['GDAX'] = True

    kraken_fees = {'take': 0.26, 'make': 0.16}
    kraken_orderbook = KrakenOrderbook(['BTC-USD', 'BCH-USD'], kraken_fees)
    active_exchanges['Kraken'] = False
    if "Kraken" in start_exchanges:
        kraken_orderbook.start_orderbook()
        active_exchanges['Kraken'] = True

    huobi_currencies = {'BTC-USD': 'btcusdt', 'BCH-USD': 'bchusdt'}
    huobi_fees = {'take': 0.2, 'make': 0.2}
    huobi_orderbook = HuobiOrderbook(['BTC-USD', 'BCH-USD'], huobi_fees)

    active_exchanges['Huobi'] = False
    if "Huobi" in start_exchanges:
        huobi_orderbook.start_orderbook()
        active_exchanges['Huobi'] = True


    print("Orderbooks started")
    unified_orderbook = UnifiedOrderbook({"Bitstamp": bitstamp_orderbook,
                                          "Bitfinex": bitfinex_orderbook,
                                          "GDAX": gdax_orderbook,
                                          "Kraken": kraken_orderbook,
                                          "Huobi": huobi_orderbook})

    orderbooks = {'Bitstamp': {'orderbook': bitstamp_orderbook, 'currencies_dict': bitstamp_currencies,
                               'creator': BitstampOrderbook, 'args': bitstamp_args,
                               'active': active_exchanges['Bitstamp'], 'fees': bitstamp_fees},
                  'GDAX': {'orderbook': gdax_orderbook, 'currencies_dict': gdax_currencies,
                           'creator': GdaxOrderbook, 'active': active_exchanges['GDAX'], 'fees': gdax_fees},
                  'Bitfinex': {'orderbook': bitfinex_orderbook, 'currencies_dict': bitfinex_currencies,
                               'creator': BitfinexOrderbook, 'active': active_exchanges['Bitfinex'], 'fees': bitfinex_fees},
                  'Kraken': {'orderbook': kraken_orderbook, 'currencies_dict': bitstamp_currencies,
                             'creator': KrakenOrderbook, 'active': active_exchanges['Kraken'], 'fees': kraken_fees},
                  'Huobi': {'orderbook': huobi_orderbook, 'currencies_dict': huobi_currencies,
                            'creator': HuobiOrderbook, 'active': active_exchanges['Huobi'], 'fees': huobi_fees},
                  'Unified': {'orderbook': unified_orderbook, 'currencies_dict': bitstamp_currencies,
                              'creator': UnifiedOrderbook, 'active': True, 'fees': dict()}}
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
    print(active_exchanges)
    print(huobi_orderbook._get_orderbook_from_exchange('BTC-USD', 3))
    print(huobi_orderbook._get_orderbook_from_exchange('BCH-USD', 3))

    app.run(host=bind_ip, port=listener_port)
