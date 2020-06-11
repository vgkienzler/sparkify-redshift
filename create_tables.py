import configparser
import psycopg2 as pg
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(f"Query {query} executed.")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(f"Query {query} executed.")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    cl_values = list(config['CLUSTER'].values())
    cl_keys = list(config['CLUSTER'].keys())
    cl_dic = dict(zip(cl_keys, cl_values))

    conn = pg.connect(f"host={cl_dic['cl_endpoint']} dbname={cl_dic['cl_db_name']} \
                      user={cl_dic['cl_user']} password={cl_dic['cl_password']} port={cl_dic['cl_port']}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    print("Tables dropped.")
    create_tables(cur, conn)
    print("Tables created.")

    conn.close()


if __name__ == "__main__":
    main()