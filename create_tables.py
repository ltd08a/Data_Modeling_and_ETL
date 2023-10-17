import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Connects to default database and establishes cursor in the database. Next, the sparkifydb database is created and a connection with a cursor is established. 
    """
    
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Failed to establish connection to the postgres database")
        print(e)

    conn.set_session(autocommit=True)
   
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Failed to establish curser to the postgres database")
        print("e")
    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Failed to drop table")
        print(e)
        
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Failed to create database")
        print(e)

    # close connection to default database
    try:
        conn.close()    
    except psycopg2.Error as e:
        print("Error: Failed to close connection to the database")
        print(e)
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Failed to connect to sparkify  database")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Failed to establish cursor to the sparkify database")
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops tables from a list
    Arguments:
    cur: establishes a cursor in the database
    conn: establishes a connection to the database
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Failed to drop table from query: {}".format(query))
            print(e)


def create_tables(cur, conn):
    """
    Creates tables from a list
    Arguments:
    cur: establishes a cursor in the database
    conn: establishes a connection to the database
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Failed to create table from query: {}".format(query))
            print(e)


def main():
    """
    Refreshes and connects to the database. Tables from a list are dropped, then created. The connection is closed. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
