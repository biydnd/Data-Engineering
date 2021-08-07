import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables prior to table creation.
        
        Parameters:
            cur: Cursor of the database Connection
            conn: Connection to postgres database
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except (Exception, psycopg2.Error) as e:
            print(e)


def create_tables(cur, conn):
    """
    Create staging_events, staging_songs, songplay, users, song, artist and time tables.
        
        Parameters:
            cur: Cursor of the database Connection
            conn: Connection to postgres database
    """
    for query in create_table_queries:
        
        try:
            cur.execute(query)
            conn.commit()
        except (Exception, psycopg2.Error) as e:
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()