# from huobi import HuobiRestClient
import client_wrapper_base
import logging
import huobi  # TODO - this dependency came from pip install huobi , maybe it should be changed

# from order_tracker import BitstampOrderTracker


class HuobiClientWrapper(client_wrapper_base.ClientWrapperBase):
    HUOBI_CURRENCIES_DICT = {'BTC': 'btc','USD':'usdt', 'BCH' : 'bch', "LTC" : 'ltc'}
    def __init__(self, credentials, orderbook, db_interface, clients_manager, supportedCurrencies = {'btc','usdt','bch','ltc'}):
        super().__init__(orderbook, db_interface, clients_manager)
        self.log = logging.getLogger(__name__)
        self._huobi_client = None
        self._signed_in_user = ""
        self._api_key = ""
        self._secret = ""
        self.set_credentials(credentials)
        self._fee = 0
        self._supportedCurrencies = supportedCurrencies 

    def set_credentials(self, client_credentials, cancel_order=True):
        super().set_credentials(client_credentials)
        self._should_have_balance = False
        username = ''
        key = ''
        secret = ''
        if cancel_order:
            self.cancel_timed_order()
        try:
            if client_credentials is not None and 'username' in client_credentials and \
                    'key' in client_credentials and 'secret' in client_credentials:
                username = client_credentials['username']
                key = client_credentials['key']
                secret = client_credentials['secret']

            if len(username) != 0 and len(username) != 0 and len(username) != 0:
                self._huobi_client = huobi.HuobiRestClient(key, secret)
                account_id = self._huobi_client.accounts().data['data'][0]['id']
                self._huobi_client.balance(account_id=account_id)
                self._signed_in_user = username
                self._api_key = key
                self._secret = secret
                self._balance_changed = True
                self._is_client_init = True
            else:
                self._signed_in_user = ''
                self._huobi_client = None
        except Exception as e:
            print("Login exception: ", e)
            self._huobi_client = None
            self._signed_in_user = ''
            self._balance_changed = False
            self._is_client_init = False

        return self._huobi_client is not None

    def get_signed_in_credentials(self):
        signed_in_dict = {True: "True", False: "False"}
        return {'signed_in_user': self._signed_in_user, 'is_user_signed_in': signed_in_dict[self._signed_in_user != ""]}

    def logout(self):
        super().logout()
        return self._huobi_client is None

    def _get_balance_from_exchange(self):
        result = {}
        if self._huobi_client is not None and self._signed_in_user != "":
            try:
                print("Getting balance from Huobi")
                account_id = self._huobi_client.accounts().data['data'][0]['id']
                bitstamp_account_balance = self._huobi_client.balance(account_id=account_id)
                print("Balance arrived from Huobi:", bitstamp_account_balance)
                balanceList = bitstamp_account_balance.data['data']['list']
                result = {}
                balances = {}
                for element in balanceList:
                    if element['currency'] in  self._supportedCurrencies or float(element['balance']) > 0 :
                        if element['type'] == 'trade':
                            balances[element['currency'] + '_trade'] = float(element['balance'])
                        else:
                            balances[element['currency']] = float(element['balance'])
                for currency in self._supportedCurrencies:
                    result[currency.upper()] = {"amount":  balances[currency + '_trade'] + balances[currency], "available": balances[currency + '_trade']}  
            except Exception as e:
                self.log.error("%s", str(e))
        return result

    def get_exchange_name(self):
        return "Huobi"

    def buy_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        # return self._execute_exchange_order('buy-ioc', execute_size_coin, price_fiat, currency_type1, currency_type2)
        return self._execute_exchange_order('buy-ioc', execute_size_coin, price, currency_from, currency_to)
        

    def sell_immediate_or_cancel(self, execute_size_coin, price, currency_from, currency_to):
        return self._execute_exchange_order('sell-ioc', execute_size_coin, price, currency_from, currency_to)



    def _execute_exchange_order(self, action_type, size, price, currency_from, currency_to):
        account_id = self._huobi_client.accounts().data['data'][0]['id']
        self.log.debug("account ID = " + str(account_id))
        execute_result = { 'order_status': False}
        if account_id == None:
            print ('ERROR empty account Id') #TODO  error handling
            execute_result['status'] = 'Error'
            execute_result['order_status'] = True
            return execute_result
        try:
            if price != None:
                exchange_result = self._huobi_client.place(account_id=str(account_id), amount=str(size), price=str(price),  symbol=currency_to + currency_from, type=action_type)
            else :
                exchange_result = self._huobi_client.place(account_id=str(account_id), amount=str(size),  symbol=currency_to + currency_from, type=action_type)


        
            orderStatus = self.order_status(exchange_result.data['data'])
            execute_result = {'exchange': self.get_exchange_name(),
                                'id': int(exchange_result.data['data']),
                                'executed_price_usd': orderStatus.data['data']['price'],
                                'currency_from' : currency_from,
                                'currency_to' : currency_to,
                                'currency1_amount' : orderStatus.data['data']['field-cash-amount'],
                                'currency2_amount' : orderStatus.data['data']['field-amount'],                                
                                'order_status': False}
            
            if orderStatus.data['data']['state'] == 'canceled':
                execute_result['status'] = "Cancelled"
            else:
                execute_result['status'] = 'Finished'
                execute_result['order_status'] = True   
        except Exception as e:
            self.log.error("%s %s", action_type, e)
            execute_result['status'] = 'Error'
            execute_result['order_status'] = True
        return execute_result
        
    
    def order_status(self, order_id): 
        result = {}
        if self._huobi_client is not None and self._signed_in_user != "":
            try:
                result = self._huobi_client.status(order_id=order_id)
            except Exception as e:
                self.log.error("%s", str(e))
        return result

    def transactions(self, transactions_limit, currencyPair = 'ethbtc'):
        states = "pre-submitted,submitted,partial-filled,partial-canceled,filled,canceled"
        try:
            if self._huobi_client is not None and self._signed_in_user != "":
                transactions = self._huobi_client.list_orders(symbol=currencyPair, states = states).data['data']
                if transactions_limit != 0 and len(transactions) > transactions_limit:
                    transactions = transactions[0:transactions_limit]
        except Exception as e:
            self.log.error("%s", str(e))
            transactions = []
        return transactions

    def minimum_order_size(self, asset_pair):
        minimum_sizes = {'BCH-BTC': 0.001, 'LTC-BTC': 0.0001}
        return minimum_sizes[asset_pair]

    def is_client_initialized(self):
        return self._is_client_init


    def sell_market(self, execute_size_coin, currency_from, currency_to = "usdt"):
        return self._execute_exchange_order('sell-market', execute_size_coin, None, currency_from, currency_to)

    def buy_market(self, execute_size_coin, currency_from, currency_to = "usdt"):
        return self._execute_exchange_order('buy-market', execute_size_coin, None, currency_from, currency_to)

    # def _order_complete(self, is_timed_order, report_status):
    #     if self._clients_manager:
    #         if is_timed_order and report_status:
    #             self._clients_manager.set_last_status(self.get_timed_order_status())
    #             print("Setting last status", self.get_timed_order_status())

    # def _create_timed_order_executer(self, asset_pair, action_type):
    #     return TimedOrderExecuter(self, self._orderbook, asset_pair)

    # def buy_limit(self, execute_size_coin, price_fiat, crypto_type):
    #     return {}

    # def sell_limit(self, execute_size_coin, price_fiat, crypto_type):
    #     return {}

    # def _cancel_order(self, order_id):
    #     return None

    # def _cancel_active_limit_order(self):
    #     pass


