import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables from S3 indicated in config file dwh.cfg
    using copy queries from copy_table_queries
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populates star schema tables with data from staging tables
    using insert queries from insert_table_queries
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
    
    # Drop Staging Tables
    for query in drop_table_queries[:2]:
        cur.execute(query)
        conn.commit()

    conn.close()


if __name__ == "__main__":
    main()