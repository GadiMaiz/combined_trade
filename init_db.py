import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
        conn = None

    return conn


def create_table(conn):
    conn.execute("CREATE TABLE sent_orders(exchange text, action_type text, crypto_size real, "
                 "price real, exchange_id integer, status text, order_time text, timed_order number, crypto_type text)")

if __name__ == '__main__':
    conn = create_connection("./Transactions.data")
    conn.execute("DROP TABLE sent_orders")
    create_table(conn)
    conn.execute("INSERT INTO sent_orders VALUES('Bitstamp', 'sell', 0.001, 9300, 1416177663, 'DONE', '2018-04-29 13:30:19.969', 1, 'BTC')")
    conn.execute("INSERT INTO sent_orders VALUES('Bitstamp', 'sell', 0.001, 10000, 1416183358, 'CANCELLED', '2018-04-29 13:40:19.969', 0, 'BTC')")
    conn.execute("INSERT INTO sent_orders VALUES('Bitstamp', 'buy', 0.001, 8000, 1416209330, 'CANCELLED', '2018-04-29 13:50:19.969', 1, 'BTC')")
    conn.commit()
    rows = conn.execute("SELECT * FROM sent_orders")
    for row in rows:
        print (row[0])


