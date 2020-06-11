"""
This module contains usefull functions to check on a redshift cluster and IAM role.
Contains the following functions:
- get_role_details()
- check_role_exists()
- get_cluster_details()
- check_cluster_exists()
- is_cluster_available()
- check_cluster_status()
"""

import botocore.exceptions as e
import boto3


def get_role_details(client, role_name):
    """
    Fetch details/parameters for role 'role_name' (string) in client 'client' (iam client object)
    List of details/parameters fetch: creation date, role id, role arn, role description.
    Print the details.
    """
    try:
        the_role = client.get_role(RoleName = role_name)
    except client.exceptions.NoSuchEntityException:
        print(f"Role {role_name} cannot be found.")
    else:   
        print(
            f"Role {role_name} exists, created on {the_role['Role']['CreateDate']}.\n"
            f"Role ID: {the_role['Role']['RoleId']}.\n"
            f"Role ARN: {the_role['Role']['Arn']}.\n"
            f"Role description: \'{the_role['Role']['Description']}\'.\n"
        )
        

def check_role_exists(client, role_name, details = False, **print_details):
    """
    Checks if an IAM role with name 'role_name' (string) exists.
    If the option to print details has been set with any of the keyword
    argument details=True, print_details=True, pd=True:
    - Prints the main parameters of the role (role name, creation date, ID and ARN)
    - Prints 'Role role_name cannot be found' if role doesn't exist
    Returns True if role exists.
    Returns False if role does not exist.
    """
    # First check if the caller wants to print the details of the role and function results
    # or not. By default they are not printed.
    print_details = {**print_details, 'details' : details}
    keywords_triggers_printing = ['details', 'print_details', 'pd']
    for key in print_details:
        if (key in keywords_triggers_printing) & (print_details[key] == True):
            details = True
    # Now collect role details.
    try:
        the_role = client.get_role(RoleName = role_name)
    except client.exceptions.NoSuchEntityException:
        if details:
            print(f"Role {role_name} cannot be found.")
        return False
    else:
        if details:
            get_role_details(client, role_name)
    return True


def get_cluster_details(client, cluster_name):
    """
    Fetch details/parameters for role 'role_name' (string) in client 'client' (iam client object)
    List of details/parameters fetch: creation date, role id, role arn, role description.
    Print the details.
    """
    try:
        client.describe_clusters(ClusterIdentifier=cluster_name)
    except e.ClientError:
        print(f"Cluster {cluster_name} cannot be found.")
    else:
        cluster = client.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]
        # If else required because ClusterCreateTime only exists once the cluster status is 'available'
        # and not when 'creating'
        if cluster['ClusterStatus'] == 'creating':
            print(f"Cluster {cluster_name} being created.")
        else :
            print(
                f"Cluster {cluster_name} exists, created on {cluster['ClusterCreateTime']}.\n"
                f"Cluster endpoint: '{cluster['Endpoint']['Address']}'."
            )
        print(
            f"Cluster DB name: {cluster['DBName']}.\n"
            f"Cluster username: {cluster['MasterUsername']}.\n"
            f"Cluster status: {cluster['ClusterStatus']}.\n"
            f"Cluster zone: {cluster['AvailabilityZone']}."
        )
            

def check_cluster_exists(client, cluster_name, details = False,  **print_details):
    """
    Checks if a cluster with 'cluster_name'(string) in 'client' (botocore.client.Redshift
    object) exists.
    If the option to print details has been set with any of the keyword
    argument details=True, print_details=True, pd=True:
    - Prints the main parameters of the cluster
    - Prints 'Cluster cluster_name cannot be found' if cluster doesn't exist
    Returns True if it exists.
    Returns False if doesn't exist.
    """
    # First check if the caller wants to print the details of the role and function results
    # or not. By default they are not printed.
    print_details = {**print_details, 'details' : details}
    keywords_triggers_printing = ['details', 'print_details', 'pd']
    for key in print_details:
        if (key in keywords_triggers_printing) & (print_details[key] == True):
            details = True
    try:
        client.describe_clusters(ClusterIdentifier=cluster_name)
    except e.ClientError:
        if details:
            print(f"Cluster {cluster_name} cannot be found.")
        return False
    else:
        if details:
            get_cluster_details(client, cluster_name)
    return True


def is_cluster_available(client, cluster_name):
    """
    Checks if cluster 'cluster_name' (string) in 'client' (botocore.client.Redshift object) is available.
    
    Returns True if cluster is available.
    
    Returns False if cluster exists but is not available.
    
    If cluster cannot be found, prints 'This cluster cannot be found.'
    Returns 1 if cluster cannot be found.
    """    
    try:
        client.describe_clusters(ClusterIdentifier=cluster_name)
    except e.ClientError:
        print("This cluster cannot be found.")
        return 1
    if client.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]['ClusterStatus'] == 'available':
        return True
    else:
        return False    


def check_cluster_status(client, cluster_name):
    """
    Checks what is the status of cluster 'cluster_name'(string) in 'client' (botocore.client.Redshift object).
    
    Returns the status if the cluster can be found.
    
    If cluster cannot be found, prints 'This cluster cannot be found.'
    Returns 1 if the cluster cannot be found.
    """
    try:
        client.describe_clusters(ClusterIdentifier=cluster_name)
        return client.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]['ClusterStatus']
    except e.ClientError:
        print("This cluster cannot be found.")
        return 1


def main():
    pass


if __name__ == "__main__":
    main()