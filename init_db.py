import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        #print(sqlite3.version)
    except Error as e:
        print(e)
        conn = None

    return conn


def create_table(conn):
    conn.execute("CREATE TABLE sent_orders(exchange text, action_type text, crypto_size real, "
                 "price real, exchange_id text, status text, order_time text, timed_order number, crypto_type text, "
                 "balance_usd number, balance_crypto number, ask real, bid real, parent_trade_order_id real, "
                 "trade_order_id real)")

if __name__ == '__main__':
    conn = create_connection("./Transactions.data")
    conn.execute("DROP TABLE sent_orders")
    create_table(conn)
    #conn.execute("UPDATE sent_orders set exchange_id = 0 where exchange_id = 'None'")
    conn.commit()
    rows = conn.execute("SELECT rowid, * FROM sent_orders")
    for row in rows:
        print (row[5])


