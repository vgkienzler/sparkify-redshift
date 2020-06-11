import configparser
import psycopg2 as pg
from sql_queries import load_staging_table_queries, insert_table_queries, remove_duplicate_users_queries, \
    remove_duplicate_artists_queries


def run_queries(cur, conn, query_list):
    """
    Run queries in query_list,
    arguments: psycopg2 cursor and connection objects, sql query lists (strings).
    """
    for query in query_list:
        print(f"Query being executed: {query}.")
        cur.execute(query)
        conn.commit()


def main():
    """
    """
    config = configparser.ConfigParser()
    # TODO: find a way to remove local reference to 'dwh.cfg', 'CLUSTER', and all the sections:
    config.read('dwh.cfg')

    cl_values = list(config['CLUSTER'].values())
    cl_keys = list(config['CLUSTER'].keys())
    cl_dic = dict(zip(cl_keys, cl_values))

    conn = pg.connect(f"host={cl_dic['cl_endpoint']} dbname={cl_dic['cl_db_name']} \
                      user={cl_dic['cl_user']} password={cl_dic['cl_password']} \
                      port={cl_dic['cl_port']}")
    cur = conn.cursor()

    print("Connection established, cursor created.")
    
    # load staging tables:
    run_queries(cur, conn, load_staging_table_queries)
    print("Staging tables loaded.")
    
    # insert data from staging tables:
    run_queries(cur, conn, insert_table_queries)
    print("Table(s) inserted.")

    # remove users and artists duplicates:
    run_queries(cur, conn, remove_duplicate_users_queries)
    run_queries(cur, conn, remove_duplicate_artists_queries)
    print("User and artists duplicates removed.")

    conn.close()


if __name__ == "__main__":
    main()