import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - drop table if exists in AWS Redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - Create table statements for AWS Redshift
    - It contains a loop executing content of file sql_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Main function that connects to the AWS Redshift and execute drop table and create table
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()