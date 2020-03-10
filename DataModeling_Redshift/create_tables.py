import configparser
import psycopg2
from sql_queries import create_schema_queries, create_table_queries, drop_table_queries



def create_schema(cur, conn):
    """ Create schema if not exists from create_schema_queries."""
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """ Drop tables to not duplicate values."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Create table from create_tables_queries."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    create_schema(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()