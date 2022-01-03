import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    - Copies data from S3 directories to the staging tables using the queries in `copy_table_queries` list.
    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    - Inserts data from the staging tables to the final tables using the queries in `insert_table_queries` list. 
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
        
    """
    - Establishes connection with the Redshift Sparkify database and gets
    cursor to it.  
    - Copies data from S3 directories to the staging tables.
    - Inserts data from the staging tables to the final tables. 
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()