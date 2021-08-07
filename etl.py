import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging_events and staging_songs tables from log files and song files
        
        Parameters:
            cur: Cursor of the database Connection
            conn: Connection to postgres database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load songplay, users, songs, artists and time tables from staging_events and staging_songs tables.
        
        Parameters:
            cur: Cursor of the database Connection
            conn: Connection to postgres database
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except (Exception, psycopg2.Error) as e:
            print(query)
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()