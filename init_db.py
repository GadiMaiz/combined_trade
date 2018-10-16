import sqlite3
from sqlite3 import Error
import logging
import os


def create_connection(db_file, log):
    """ create a database connection to a SQLite database """
    try:
        db_conn = sqlite3.connect(db_file)
    except Error as e:
        log.error(e)
        db_conn = None

    return db_conn


def create_table(db_conn):
    db_conn.execute("CREATE TABLE sent_orders(exchange text, action_type text, size real, "
                    "price real, exchange_order_id text, status text, order_time text, timed_order number, "
                    "currency_to text, balance_currency_from number, balance_currency_to number, ask real, bid real, "
                    "parent_trade_order_id real, trade_order_id real, user_id text, external_order_id text,"
                    "user_quote_price real, currency_from text)")


def init_db(data_dir, db_filename):
    log = logging.getLogger(__name__)
    db_path = os.path.join(data_dir, db_filename)
    directory_exists = True
    if not os.path.exists(data_dir):
        directory_exists = False
        try:
            log.info("Creating directory for data in path: <%s>", data_dir)
            os.makedirs(data_dir)
            directory_exists = True
        except Exception as e:
            log.error("Can't create directory in path <%s>, exception: <%s>", data_dir, e)
    elif not os.path.isdir(data_dir):
        directory_exists = False
        log.error("File exists instead of directory in path <%s>", data_dir)

    db_init = False
    if directory_exists:
        need_to_create_db = False
        if not os.path.exists(db_path):
            need_to_create_db = True
            log.info("Creating DB in path: <%s>", db_path)
        try:
            conn = create_connection(db_path, log)
            if need_to_create_db:
                create_table(conn)

            rows = conn.execute("SELECT COUNT(*) AS number_of_rows FROM sent_orders")
            rows_num = -1
            for row in rows:
                rows_num = row[0]
            log.info("DB exists with <%d> records", rows_num)
            db_init = True
        except Exception as e:
            log.error("Error connecting to DB file in path: <%s>, need_to_create_db: <%s>, exception: <%s>", db_path,
                      need_to_create_db, e)
    return db_init

"""if __name__ == '__main__':
    conn = create_connection("./Transactions.data")
    conn.execute("DROP TABLE sent_orders")
    create_table(conn)
    #conn.execute("UPDATE sent_orders set exchange_id = 0 where exchange_id = 'None'")
    conn.commit()
    rows = conn.execute("SELECT rowid, * FROM sent_orders")
    for row in rows:
        print (row[5])"""


