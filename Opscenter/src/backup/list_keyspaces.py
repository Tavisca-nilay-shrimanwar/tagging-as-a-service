import json, os
import boto3
from context import common, alerts

region_name = common.get_region()

# TODO: Take these below variables from SSM
BUCKET = 'cassandra-opscenter-metadata-bucket'
FILE_TO_READ = 'DataAPI_backup_metadata.json'
from boto3 import client

client = client('s3')

# TODO: Take these below variables from SSM
BUCKET = 'cassandra-opscenter-metadata-bucket'
FILE_TO_READ = 'poc_opscenter_metadata.json'
from boto3 import client

client = client('s3')

# Reading File from s3
result = client.get_object(Bucket=BUCKET, Key=FILE_TO_READ) 
text = result["Body"].read().decode()

# Parsing into Python Dictinary
parsed_dict = json.loads(text)

#Setting Variables
cluster_name = parsed_dict['POC']['opscenter_cluster_name']
opscenter_ip = f"{parsed_dict['POC']['opscenter_ip']}:{parsed_dict['POC']['opscenter_port']}"

ssm_client = boto3.client("ssm", region_name=region_name)
log_group_name = common.get_parameter_from_ssm("cassandra-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "take-backup",
    log_group_name = log_group_name
).get_logger()


authentication_response = common.authenticate(opscenter_ip, logger)
session_id = authentication_response['sessionid']
print("Authentication complete. Session Id: {0}".format(session_id))

keyspaces = list(common.get_all_keyspaces_for_cluster(cluster_name, opscenter_ip, session_id))

# Setting keyspaces as output parameter
os.system(f"echo 'Keyspaces={','.join(keyspaces)}' >> $GITHUB_OUTPUT")