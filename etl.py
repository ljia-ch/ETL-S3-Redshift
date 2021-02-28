import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 storage to Redshift staging tables: staging_events, staging_songs
    
    Parameters: 
        cur (class cursor): to run query on a database connection
        conn (class connection): connection to PostgreSQL database instance
        
    Returns: nothing
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables (staging_events and staging_songs) to star schema (songs, artists, users, time and songplays)
    
    Parameters: 
        cur (class cursor): to run query on a database connection
        conn (class connection): connection to PostgreSQL database instance
        
    Returns: nothing
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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