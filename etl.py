import configparser
import psycopg2 as pg
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"Query being executed: {query}.")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Query being executed: {query}.")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    cl_values = list(config['CLUSTER'].values())
    cl_keys = list(config['CLUSTER'].keys())
    cl_dic = dict(zip(cl_keys, cl_values))

    conn = pg.connect(f"host={cl_dic['cl_endpoint']} dbname={cl_dic['cl_db_name']} \
                      user={cl_dic['cl_user']} password={cl_dic['cl_password']} \
                      port={cl_dic['cl_port']}")
    cur = conn.cursor()

    print("Connection established, cursor created.")
    
    load_staging_tables(cur, conn)
    
    print("Staging tables loaded.")
    
    insert_tables(cur, conn)
    
    print("Table(s) inserted.")

    conn.close()


if __name__ == "__main__":
    main()