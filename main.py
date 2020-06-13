import create_role_cluster as create_rc
import check_role_cluster as check_rc
import create_tables
import sys
import time
import etl


def advanced_input(authorised_input):
    """
    Print the message 'Please enter you choice:' and wait for the user's input.
    User must enter a string which is part of authorised_input.
    If the input is not in list authorised_input, raises an exception and ask for a new input.
    If user's input is in 'authorised_input' returns the user's input.
    """
    while True:
        try:
            user_input = input("Please enter you choice:\n")
            assert user_input in authorised_input
        except (ValueError, AssertionError):
            print("Input Error: only '", end='')
            print(*authorised_input, sep="', '", end="'")
            print(" are valid inputs.\nPlease enter a valid input.\n")
        else:
            return user_input


def main():
    """Launches all the steps required to create the data tables in redshift."""

    # Launches the creation of an IAM role and redshift clusters if they don't already exists:
    client, cluster_name = create_rc.main()

    # Check redshift cluster's availability. If available, create the tables:
    cluster_status = check_rc.check_cluster_status(client, cluster_name)

    if cluster_status == 'creating':
        print(f"Cluster '{cluster_name}' is being created.\n" +
              "This can take several minutes. Do you want to Wait (W) or Exit (Q)?\n" +
              "Exiting won't interrupt cluster creation. You can resume later by re-launching main.py")
        valid_choices = ['q', 'Q', 'w', 'W']
        waiting = advanced_input(valid_choices)

        if waiting.lower() == 'q':
            sys.exit(0)
        elif waiting.lower() == 'w':
            print("Waiting...")
            while cluster_status == 'creating':
                time.sleep(20)
                cluster_status = check_rc.check_cluster_status(client, cluster_name)
                print(f"Waiting... cluster status: {cluster_status}.")

    if cluster_status == 'available':
        cluster = client.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]
        # TODO: remove local reference to 'dwh.cfg'.
        create_rc.update_section_key('dwh.cfg', 'CLUSTER', 'cl_endpoint', cluster['Endpoint']['Address'])

        # When cluster available ask the user if she wants to launch the etl process:
        print(f"Cluster '{cluster_name}' available.\n" +
              "Do you want to create tables and launch the ETL process? Yes (Y), No (n)?\n" +
              "This will drop existing tables, re-create them and load data.")
        valid_choices = ['y', 'Y', 'n', 'N']
        launch_etl = advanced_input(valid_choices)
        if launch_etl.lower() == 'y':
            create_tables.main()
            etl.main()
        else:
            sys.exit(0)

    else:
        print(f"Cluster '{cluster_name}' current status: '{cluster_status}'.\n"
              "Please activate or repair the cluster and relaunch the program.\n"
              "Exiting.")
        sys.exit(1)

    print("The End.")


if __name__ == "__main__":
    main()
