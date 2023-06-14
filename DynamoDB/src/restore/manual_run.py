import boto3
from datetime import datetime
from context import common, alerts
import os, sys, traceback
from utils import fetch_and_validate_metadata, restore_from_latest_arn

region_name = "us-east-1"

# Initializing SSM Client
ssm_client = boto3.client("ssm", region_name=region_name)
log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "mannual-restore",
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

try:
    dynamodb_client = boto3.client('dynamodb', region_name) 
    source_table_name = os.environ.get("TableNameForBackup")
    target_table_name = "restored_"+source_table_name+"_"+datetime.now().strftime("%m-%d-%Y-%H-%M-%S")

    # Setting target table name as output parameter
    os.system(f"echo 'TargetTableName={target_table_name}' >> $GITHUB_OUTPUT")
    
    backup_type = os.environ.get("DynamoDBRestoreMethod")

    s3_client = boto3.client('s3', region_name)

    # To deploy via manual snapshot
    if backup_type == 'Manual':
        backup_arn = os.environ.get("BackupArn")
        restored_table = dynamodb_client.restore_table_from_backup(
                                TargetTableName=target_table_name,
                                BackupArn=backup_arn
                        )
        success_title = "DynamoDB Manual Restore Operation Started"
        success_message = f"DynamoDB Restore process started from backup ARN: {backup_arn}, Name of the restored table is: {restored_table['TableDescription']['TableName']}"
        
        # Logging Successful Restore
        logger.info(success_message)

        # Sending Teams Message for Successful Restore
        teams_messenger.send_message(title=success_title, message=success_message)
        
        # Sending Email for Successful Restore
        emailer.send_success_email(
            subject=success_title,
            content=success_message
        )
        
        # Validating from metadata
        metadata_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
        metadata_file_key = common.get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)

        if common.object_exists_in_s3(metadata_bucket_name, metadata_file_key, s3_client):
            fetch_and_validate_metadata(metadata_bucket_name, metadata_file_key, source_table_name, backup_arn, target_table_name, dynamodb_client, s3_client, logger, emailer, teams_messenger)

        else:
            logger.info("No Metadata found")

    elif backup_type == 'ManualLatest':
        restore_from_latest_arn(dynamodb_client, region_name, logger, emailer, teams_messenger)

    # To deploy via PITR method by specifying date and time
    elif backup_type == 'Pitrdate':

        restore_date_time = datetime.strptime(os.environ.get("PITRBackupDate"), "%Y-%m-%d::%H:%M")
        restored_table = dynamodb_client.restore_table_to_point_in_time(
                                SourceTableName=source_table_name,
                                TargetTableName=target_table_name,
                                RestoreDateTime=restore_date_time
                            )

        success_title = "DynamoDB PITR Restore Operation Started!"
        success_message = f"Restore started for the dynamodb table from PITR selected date: {restore_date_time}, Name of the restored table is: {restored_table['TableDescription']['TableName']}"

        # Logging Successful Restore
        logger.info(success_message)

        # Sending Teams Message for Successful Restore
        teams_messenger.send_message(title=success_title, message=success_message)

        # Sending Email for Successful Restore
        emailer.send_success_email(
            subject=success_title,
            content=success_message
        )

    # To deploy via PITR with latest snapshot
    elif backup_type == 'Pitr':
        restored_table = dynamodb_client.restore_table_to_point_in_time(
                                SourceTableName=source_table_name,
                                TargetTableName=target_table_name,
                                UseLatestRestorableTime=True
                                )

        success_title = "DynamoDB PITR Restore Operation Started!"
        success_message = f"Restoreoperation started for the dynamodb table from PITR from the latest backup, Name of the restored table is: {restored_table['TableDescription']['TableName']}"
        
        # Logging Successful Restore
        logger.info(success_message)

        # Sending Teams Message for Successful Restore
        teams_messenger.send_message(title=success_title, message=success_message)
        
        # Sending Email for Successful Restore
        emailer.send_success_email(
            subject=success_title,
            content=success_message
        )
    else:
        failure_title = "DynamoDB Restore Operation Failed"
        failure_message = f"Restore failed For table {source_table_name}: Please Specify valid Restore Method"
        
        # Logging Failed Restore
        logger.error(failure_message)

        # Sending Teams Message for Failed Restore
        teams_messenger.send_message(title=failure_title, message=failure_message)

        # Sending Email for Failed Restore
        emailer.send_failure_email(
            subject=failure_title,
            content=failure_message
        )

        sys.exit(1)

except dynamodb_client.exceptions.TableAlreadyExistsException:
    failure_title = "DynamoDB Restore operation Failed"
    failure_message = f"Restore failed For table {source_table_name}: Table with the same name already exist"
    
    # Logging Failed Restore
    logger.error(failure_message)

    # Sending Teams Message for Failed Restore
    teams_messenger.send_message(title=failure_title, message=failure_message)
    
    # Sending Email for Failed Restore
    emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )

    sys.exit(1)

except dynamodb_client.exceptions.InvalidRestoreTimeException:
    failure_title = "DynamoDB Restore operation Failed"
    failure_message = f"Restore failed For table {source_table_name}: No backup found for the specified date"
    
    # Logging Failed Restore
    logger.error(failure_message)

    # Sending Teams Message for Failed Restore
    teams_messenger.send_message(title=failure_title, message=failure_message)
    
    # Sending Email for Failed Restore
    emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )

    sys.exit(1)
   
except dynamodb_client.exceptions.TableNotFoundException:
    failure_title = "DynamoDB Restore operation Failed"
    failure_message = f"Restore failed For table {source_table_name}: No source table found with the specified name"

    # Logging Failed Restore
    logger.error(failure_message)

    # Sending Teams Message for Failed Restore
    teams_messenger.send_message(title=failure_title, message=failure_message)

    # Sending Email for Failed Restore
    emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )

    sys.exit(1)

except dynamodb_client.exceptions.BackupInUseException:
    failure_title = "DynamoDB Restore operation Failed"
    failure_message = f"Restore failed For table {source_table_name}: The provided ARN is already in use"
    
    # Logging Failed Restore
    logger.error(failure_message)

    # Sending Teams Message for Failed Restore
    teams_messenger.send_message(title=failure_title, message=failure_message)
    
    # Sending Email for Failed Restore
    emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )

    sys.exit(1)


except Exception as e:

    failure_title = "DynamoDB Restore operation Failed"
    ex_type, ex, tb = sys.exc_info()
    failure_message = f"Restore failed For table {source_table_name}: {ex_type}: {ex}\nTraceback: {traceback.format_exc()}"

    # Logging Failed Restore
    logger.error(failure_message)

    # Sending Teams Message for Failed Restore
    teams_messenger.send_message(title=failure_title, message=failure_message)
    
    # Sending Email for Failed Restore
    emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )

    traceback.print_tb(tb)
    sys.exit(1)
