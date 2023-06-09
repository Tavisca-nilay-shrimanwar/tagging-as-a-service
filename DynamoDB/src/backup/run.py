from cmath import log
import boto3
import os, sys, traceback
from utils import create_metadata_before_backup, write_metadata_to_s3
from datetime import datetime
from context import common, alerts

region_name = "us-east-1"

# Initializing SSM Client
ssm_client = boto3.client("ssm", region_name=region_name)
log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "take-backup",
    log_group_name = log_group_name
).get_logger()

# Initiating teams messenger
teams_messenger = alerts.TeamsMessenger(webhook_list=["https://trilegiant.webhook.office.com/webhookb2/24b3f38b-6a41-4216-b747-d7693fc34b46@be80116c-1704-4639-8c7f-77ded4343d23/IncomingWebhook/fdfe1ef9ebe549fabc660d7e1f189e33/ce7ec5b7-b9c4-47d6-a605-61bcd8f1cb11"], log_grp_name=log_group_name)

alert_email_sender = common.get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
alert_email_receiver = common.get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

emailer = alerts.Emailer(
    sender = alert_email_sender,
    receiver =alert_email_receiver.strip().split(",")
)

# Take backup
try:
    dynamodb_client = boto3.client('dynamodb', region_name)

    # Fetching 3 fields in dictionary: records, GSI, LSI
    table_name = os.environ.get("TableNameForBackup")
    metadata = create_metadata_before_backup(table_name, region_name)

    backupresponse = dynamodb_client.create_backup(
        TableName=table_name,
        BackupName="backup_"+table_name+"_"+datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
    )
    
    success_title = "DynamoDB Backup Operation Successul"
    success_message = f"Backup Successfully Created for table {table_name}: {backupresponse['BackupDetails']['BackupArn']}"
    
    # Logging Success
    logger.info(success_message)

    # Sending Teams Message
    teams_messenger.send_message(title=success_title, message=success_message)

    # Sending Email for Failure
    emailer.send_success_email(
        subject=success_title,
        content=success_message
    )

    metadata[table_name][0]["backup_arn"] = backupresponse['BackupDetails']['BackupArn']
    metadata[table_name][0]["timestamp"] = datetime.now().timestamp()
    metadata[table_name][0]["backup_retention_days"] = os.environ.get("BackupRetentionPeriod")
    
    metadata_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
    metadata_file_key = common.get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)
    
    # Updating metadata dictionary with Backup ARN and write/update in metadata.json file in S3
    response = write_metadata_to_s3(metadata, table_name, region_name, metadata_bucket_name, metadata_file_key, logger, teams_messenger)

except (dynamodb_client.exceptions.TableNotFoundException, dynamodb_client.exceptions.ResourceNotFoundException):
    
    error_title = "DynamoDB Backup Operation Failed!"
    error_message = f'No source table found with the specified name: {table_name}'
    
    # Logging Failure
    logger.error(error_message)

    # Sending Teams Message
    teams_messenger.send_message(title=error_title, message="Backup of DynamoDB table {table_name} failed\nReason: {error_message}")

    # Sending Email for Failure
    emailer.send_failure_email(
                    subject=error_title,
                    content=f"Backup of DynamoDB table {table_name} failed\nReason: {error_message}"
                )
    sys.exit(1)

except Exception as e:
    ex_type, ex, tb = sys.exc_info()

    error_title = "DynamoDB Backup Operation Failed!"
    error_message = f"{ex_type}: {ex}"
    
    # Logging Failure
    logger.error(error_message)

    # Sending Teams Message
    teams_messenger.send_message(title=error_title, message="Backup of DynamoDB table {table_name} failed\nReason: {error_message}\nTraceback: {traceback.format_exc()")

    # Sending Email for Failure
    emailer.send_failure_email(
                    subject=error_title,
                    content=f"Backup of DynamoDB table {table_name} failed\nReason: {error_message}\nTraceback: {traceback.format_exc()}"
                )
    traceback.print_tb(tb)
    sys.exit(1)
