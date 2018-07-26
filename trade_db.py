import sqlite3
import logging
import re
import datetime

class TradeDB:
    def __init__(self, db_file):
        self._db_file = db_file
        self.log = logging.getLogger(__name__)

    def create_db_connection(self, db_file):
        """ create a database connection to a SQLite database """
        try:
            conn = sqlite3.connect(db_file)
        except sqlite3.Error as e:
            self.log.error("db connection error",e)
            conn = None

        return conn

    def write_order_to_db(self, order_info):
        conn = self.create_db_connection(self._db_file)
        if conn is None:
            self.log.error("Can't connect to DB")
        else:
            try:
                if 'ask' not in order_info or 'bid' not in order_info:
                    order_info['ask'] = 0
                    order_info['bid'] = 0
                if 'balance' not in order_info or 'balances' not in order_info['balance']:
                    order_info['balance'] = {'balances': {'USD': {'available': 0},
                                                          order_info['crypto_type']: {'available': 0}}}
                insert_str = "INSERT INTO sent_orders VALUES('{}', '{}', {}, {}, '{}', '{}', '{}', {}, '{}', {}, {}, {}" \
                             ",{})".format(order_info['exchange'], order_info['action_type'], order_info['crypto_size'],
                                          order_info['price_fiat'], order_info['exchange_id'], order_info['status'],
                                          order_info['order_time'], order_info['timed_order'],
                                          order_info['crypto_type'],
                                          order_info['balance']['balances']['USD']['available'],
                                          order_info['balance']['balances'][order_info['crypto_type']]['available'],
                                          order_info['ask'], order_info['bid'])
                print(insert_str)
                self.log.info(insert_str)
                conn.execute(insert_str)
                conn.commit()
            except Exception as e:
                self.log.error("DB error: <%s>", str(e))

    def get_sent_orders(self, orders_limit, filter):
        conn = self.create_db_connection(self._db_file)
        limit_clause = ''
        if orders_limit > 0:
            limit_clause = " LIMIT " + str(orders_limit)
        where_clause = ""

        if filter:
            if 'exchanges' in filter:
                where_clause = 'exchange in ('
                first_exchange = True
                exchange_re = re.compile('^[a-zA-Z]+$')
                for exchange in filter['exchanges']:
                    if exchange_re.match(exchange):
                        if first_exchange:
                            first_exchange = False
                        else:
                            where_clause += ', '

                        where_clause += '\'{}\''.format(exchange)
                where_clause += ') '
            if 'start_date' in filter:
                try:
                    start_date = datetime.datetime.strptime(filter['start_date'], '%Y-%m-%d %H:%M')
                    if where_clause != "":
                        where_clause += " AND "
                    where_clause += 'datetime(order_time) >= datetime(\'{}\')'.format(start_date)
                except Exception as e:
                    pass

            if 'end_date' in filter:
                try:
                    end_date = datetime.datetime.strptime(filter['end_date'], '%Y-%m-%d %H:%M')
                    if where_clause != "":
                        where_clause += " AND "
                    where_clause += 'datetime(order_time) <= datetime(\'{}\')'.format(end_date)
                except Exception as e:
                    pass

            if 'statuses' in filter:
                if where_clause != "":
                    where_clause += " AND "
                where_clause += 'status in ('
                first_status = True
                status_re = re.compile('^[a-zA-Z ]+$')
                for status in filter['statuses']:
                    if status_re.match(status):
                        if first_status:
                            first_status = False
                        else:
                            where_clause += ', '
                        where_clause += '\'{}\''.format(status)
                where_clause += ') '

            if 'types' in filter:
                if where_clause != "":
                    where_clause += " AND "
                where_clause += 'action_type in ('
                first_type = True
                type_re = re.compile('^[a-zA-Z _]+$')
                for action_type in filter['types']:
                    if type_re.match(action_type):
                        if first_type:
                            first_type = False
                        else:
                            where_clause += ', '
                        where_clause += '\'{}\''.format(action_type)
                where_clause += ') '
        if where_clause != "":
            where_clause = "WHERE {}".format(where_clause)
        query = "SELECT * FROM (SELECT * FROM sent_orders ORDER BY datetime(order_time) DESC) " + where_clause + \
                limit_clause
        sent_orders = conn.execute(query)
        all_orders = []
        for curr_order in sent_orders:
            exchange_id = curr_order[4]
            if exchange_id is None:
                exchange_id = ""
            order_dict = {'exchange': curr_order[0],
                          'action_type': curr_order[1],
                          'crypto_size': curr_order[2],
                          'price_fiat': curr_order[3],
                          'exchange_id': exchange_id,
                          'status': curr_order[5],
                          'order_time': curr_order[6],
                          'timed_order': curr_order[7],
                          'crypto_type': curr_order[8],
                          'usd_balance': curr_order[9],
                          'crypto_available': curr_order[10],
                          'ask': curr_order[11],
                          'bid': curr_order[12]}
            all_orders.append(order_dict)
        conn.close()
        return all_orders