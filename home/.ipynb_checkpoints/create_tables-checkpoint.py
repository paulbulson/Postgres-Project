import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ 
    Connects to Postgres, creates the database, and returns both the connection and the connection cursor. Any existing database will be dropped
    and all data lost for eternity.
  
    Parameters: 
    None
    
    Returns: 
    cur (psycopg2.extensions.cursor): Cursor for a connection to a Postgres database
    connection (psycopg2.extensions.connection): Connection to a Postgres database
    """

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ 
    Drops the table via the sql (found in sql_queries.py) refenced by the drop_table_queries list.
  
    Parameters: 
    None
    
    Returns: 
    None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Creates the table via the sql (found in sql_queries.py) refenced by the create_table_queries list.
  
    Parameters: 
    None
    
    Returns: 
    None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Drops the existing tables and creates new tables. 
  
    Parameters: 
    None
    
    Returns: 
    Nothing
    """

    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()