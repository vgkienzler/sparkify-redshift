"""
This modules creates a redshift cluster and the corresponding roles that will receive the tables and data.
Contains the following functions:
- update_section_key()
- create_iam_role()
- attach_policy_to_role()
- create_redshift_cluster()
- main()
and the following class:
- AwsCredentials.
"""
import sys
import boto3
import configparser
import check_role_cluster as crc
import json

CONFIG_FILE_NAME = 'dwh.cfg'
CONFIG_SECRET_FILE_NAME = 'aws-secret.cfg'
# TODO: see if there is a way to replace local reference to sections/keys and centralise these.


def update_section_key(file_name, section, key, value):
    """
    Updates the value of 'key' in section 'section' to 'value', in file 'file_name'
    using ConfigParser().
    Takes 4 arguments: file_name (string), section (string), key (string), value (string).
    Returns 0 if update works
    """
    try:
        config = configparser.ConfigParser()
        config.read(file_name)

        # Update the 'key' in 'section' with the new 'value'
        config[section][key] = value

        # Write the new configuration to the file (update)
        with open(file_name, "w+") as configfile:
            config.write(configfile)

        print(f"Key '{key}' of section '{section}' in file '{file_name}' updated with new value '{value}'.")
        return 0
    except Exception:
        return 1


def create_iam_role(iam_client, role_name):
    """
    Creates an IAM role based on the iam_client (IAM client object) and role_name (string)
    provided as arguments.
    Returns the arn for the role if creating is successful.
    Returns 1 if an exception is raised.
    """
    try:
        print('Creating a new IAM Role')
        iam_client.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        print(f"Role {role_name} created successfully.")

        # Get, print and return the IAM role ARN.
        role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
        print(f"Returning IAM role ARN: {role_arn}")
        return role_arn

    except Exception as e:
        print(e)
        return 1


def attach_policy_to_role(iam_client, policy_arn, role_name):
    """
    Attache policy_arn (string) to role_name (string) via
    iam_client.attach_role_policy method.
    arg iam_client: botocore.client.IAM object
    """
    iam_client.attach_role_policy(RoleName=role_name,
                                  PolicyArn=policy_arn
                                  )['ResponseMetadata']['HTTPStatusCode']
    print('Policy Attached')


def create_redshift_cluster(parser, section_name, redshift_client, cluster_id, cluster_role_arn):
    """
    Creates a redshift cluster with identifier 'cluster_id' using the following information:
    - configuration information from parser (ConfigParser object),
    - redshift client redshift_client (Redshift client object),
    - AWS IAM role arn cluster_role_arn (string)
    """
    try:
        response = redshift_client.create_cluster(
            # Hardware specifications:
            ClusterType=parser.get(section_name, 'cl_type'),
            NodeType=parser.get(section_name, 'node_type'),
            NumberOfNodes=parser.getint(section_name, 'num_nodes'),

            # Identifiers and Credentials:
            DBName=parser.get(section_name, 'cl_db_name'),
            ClusterIdentifier=cluster_id,
            MasterUsername=parser.get(section_name, 'cl_user'),
            MasterUserPassword=parser.get(section_name, 'cl_password'),

            # Roles:
            IamRoles=[cluster_role_arn]
        )
        return response
    except Exception as e:
        print(e)
        return 1


class AwsCredentials:
    """
    AWS connection, contains two attributes (key and secret key), the credentials  of a user with
    access to AWS db.
    Upon init, check if config file passed in argument exist and extract the key and secret key from
    the file. Print error messages if the file specified doesn't exist or if the section 'AWS' and
    options 'key', 'secret' don't exist in the config file.
    """
    def __init__(self, file):
        self.file = file
        self.key = ''
        self.secret = ''
        self.section = 'AWS'
        self.option_key = 'key'
        self.option_secret = 'secret'

        # Read and extract data form config file:
        try:
            with open(self.file) as config_file:
                parser = configparser.ConfigParser()
                parser.read_file(config_file)

            self.key = parser.get(self.section, self.option_key)
            self.secret = parser.get(self.section, self.option_secret)
        except FileNotFoundError:
            print(f"Config file '{self.file}' not found. \nExiting.")
            sys.exit(1)
        except configparser.NoSectionError:
            print(f"Section '{self.section}' does not exist in file '{self.file}'."
                  f" Please check your configuration file. Exiting.")
            sys.exit(1)
        except configparser.NoOptionError as e:
            print(f"Section '{e.option}' does not exist in file '{self.file}'."
                  f" Please check your configuration file. Exiting.")
            sys.exit(1)


def main():
    """
    """
    aws_cred = AwsCredentials(CONFIG_SECRET_FILE_NAME)

    # Read and extract data from non secret config file.
    with open(CONFIG_FILE_NAME) as config_file:
        config = configparser.ConfigParser()
        config.read_file(config_file)

    aws_key = aws_cred.key
    aws_secret = aws_cred.secret

    aws_region = config.get('AWS', 'region')

    print(f"Config data extracted from '{CONFIG_SECRET_FILE_NAME} and {CONFIG_FILE_NAME}'.")

    # Creates IAM client
    iam = boto3.client('iam',
                       region_name=aws_region,
                       aws_access_key_id=aws_key,
                       aws_secret_access_key=aws_secret
                       )
    print("Boto3 IAM client created.")

    # Create IAM role if role doesn't already exist:
    role_name_iam = config['IAM_ROLE']['iam_role_name']
    if not crc.check_role_exists(iam, role_name_iam, print_details=False):
        arn_role_iam = create_iam_role(iam, role_name_iam)
        print(f"Role '{role_name_iam}' created.")
    else:
        arn_role_iam = iam.get_role(RoleName=role_name_iam)['Role']['Arn']
        print(f"Role '{role_name_iam}' already exists, not creating a new role.")

    # Update roleArn in dwh.cfg
    update_section_key(CONFIG_FILE_NAME, 'IAM_ROLE', 'arn', arn_role_iam)
    print(f"Config file '{CONFIG_FILE_NAME}' updated with role arn.")

    # Attach a policy to the role
    attach_policy_to_role(iam, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", role_name_iam)

    # Create redshift client
    redshift = boto3.client('redshift',
                            region_name=aws_region,
                            aws_access_key_id=aws_key,
                            aws_secret_access_key=aws_secret
                            )
    print("Boto3 redshift client created.")

    # Create cluster cl-sparkify if it doesn't already exist:
    cluster_name = config['CLUSTER']['cl_identifier']
    if not crc.check_cluster_exists(redshift, cluster_name, details=False):
        create_redshift_cluster(config, 'CLUSTER', redshift, cluster_name, arn_role_iam)
        print(f"Cluster '{cluster_name}' created. Wait for availability.")
    else:
        print(f"Cluster '{cluster_name}' already exists, not creating a new cluster.")

    return redshift, cluster_name


if __name__ == "__main__":
    client, cluster_name = main()
    print(f"Client: '{client}', Cluster name: '{cluster_name}'.")  # Only for debug.
