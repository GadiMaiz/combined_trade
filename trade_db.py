import sqlite3
import logging

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
                insert_str = "INSERT INTO sent_orders VALUES('{}', '{}', {}, {}, {}, '{}', '{}', {}, '{}', {}, " \
                             "{})".format(order_info['exchange'], order_info['action_type'], order_info['crypto_size'],
                                          order_info['price_fiat'], order_info['exchange_id'], order_info['status'],
                                          order_info['order_time'], order_info['timed_order'],
                                          order_info['crypto_type'],
                                          order_info['balance']['balances']['USD']['available'],
                                          order_info['balance']['balances'][order_info['crypto_type']]['available'])
                self.log.info(insert_str)
                conn.execute(insert_str)
                conn.commit()
            except Exception as e:
                self.log.error("DB error: <%s>", str(e))

    def get_sent_orders(self, orders_limit):
        conn = self.create_db_connection(self._db_file)
        limit_clause = ''
        if orders_limit > 0:
            limit_clause = " LIMIT " + str(orders_limit)
        sent_orders = conn.execute("SELECT * FROM (SELECT * FROM sent_orders ORDER BY datetime(order_time) DESC)" +
                                   limit_clause)
        all_orders = []
        for curr_order in sent_orders:
            exchange_id = curr_order[4]
            if exchange_id is None:
                exchange_id = ""
            order_dict = {'exchange' : curr_order[0],
                          'action_type' : curr_order[1],
                          'crypto_size': curr_order[2],
                          'price_fiat': curr_order[3],
                          'exchange_id': exchange_id,
                          'status': curr_order[5],
                          'order_time' : curr_order[6],
                          'timed_order' : curr_order[7],
                          'crypto_type': curr_order[8],
                          'usd_balance': curr_order[9],
                          'crypto_available': curr_order[10]}
            all_orders.append(order_dict)
        conn.close()
        return all_orders