import json, os, sys
from context import common, alerts
from utils import restore_after_configuring_destination
import boto3
from datetime import datetime

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
cluster_name = os.environ.get("ClusterNameForBackup") # This should be from new cluster!
target_cluster_name = "restored"+os.environ.get("ClusterNameForBackup")+datetime.now().strftime("%m-%d-%Y-%H-%M-%S")

# Setting target table name as output parameter
os.system(f"echo 'TargetClusterName={target_cluster_name}' >> $GITHUB_OUTPUT")

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

restore_type = os.environ.get('CassandraClusterRestoreMethod')

if restore_type == "Manual":
    backup_id = os.environ.get("BackupId")
elif restore_type == "ManualLatest":
    s3_backups = common.get_backups_on_s3(cluster_name, opscenter_ip, session_id, logger)
    if len(s3_backups) > 0:
        backup_id = sorted(s3_backups, key=lambda _: _['event_time'], reverse=True)[0]['backup_id']
    else:
        logger.error("No Backup Found on S3")
        sys.exit(1)

restore_scope = os.environ.get("RestoreScope")


# TODO: Pass target_cluster_name here after we incorporate cluster provisoining infra
if restore_scope == "All":
    keyspaces = 'All'
    restore_after_configuring_destination(cluster_name, opscenter_ip, session_id, bucket_name, keyspaces, backup_id, logger, restore_scope)
elif restore_scope == "Keyspace":
    keyspaces = os.environ.get("Keyspaces").split(',')
    restore_after_configuring_destination(cluster_name, opscenter_ip, session_id, bucket_name, keyspaces, backup_id, logger, restore_scope)
else:
    keyspaces = [os.environ.get("Keyspaces")]
    table_name = os.environ.get("TableName")
    restore_after_configuring_destination(cluster_name, opscenter_ip, session_id, bucket_name, keyspaces, backup_id, logger, restore_scope, table_name)