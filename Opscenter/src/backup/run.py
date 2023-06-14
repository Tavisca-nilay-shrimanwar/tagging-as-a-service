import json, os
from context import common, alerts
from utils import create_immediate_bakup_after_configuring_destination
import boto3

region_name = common.get_region()

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
cluster_name = os.environ.get("ClusterNameForBackup")#parsed_dict['DataAPI']['opscenter_cluster_name']
opscenter_ip = f"{parsed_dict['POC']['opscenter_ip']}:{parsed_dict['POC']['opscenter_port']}"
bucket_name = parsed_dict['POC']['s3_backup_path']

# Initializing SSM Client
ssm_client = boto3.client("ssm", region_name=region_name)
log_group_name = common.get_parameter_from_ssm("cassandra-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "take-backup",
    log_group_name = log_group_name
).get_logger()

alert_email_sender = common.get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
alert_email_receiver = common.get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

emailer = alerts.Emailer(
    sender = alert_email_sender,
    receiver =alert_email_receiver.strip().split(",")
)

authentication_response = common.authenticate(opscenter_ip, logger)
session_id = authentication_response['sessionid']
print("Authentication complete. Session Id: {0}".format(session_id))
keyspaces = os.environ.get("CassandraKeySpacesNames").split(',')

create_immediate_bakup_after_configuring_destination(cluster_name, opscenter_ip, bucket_name, session_id, keyspaces, logger)