import boto3
from datetime import datetime
from context import common, alerts
import os, sys, traceback
from utils import fetch_and_validate_metadata, restore_from_latest_arn

region = "us-east-1"

# Initializing SSM Client
ssm_client = boto3.client("ssm", region_name=region)
log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "mannual-restore",
    log_group_name = log_group_name
).get_logger()

alert_email_sender = common.get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
alert_email_receiver = common.get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

emailer = alerts.Emailer(
    sender = alert_email_sender,
    receiver =alert_email_receiver.strip().split(",")
)

try:
    dynamodb_client = boto3.client('dynamodb', region) 
    source_table_name = os.environ.get("TableNameForBackup")
    target_table_name = "restored_"+source_table_name+"_"+datetime.now().strftime("%m-%d-%Y-%H-%M-%S")

    # Setting target table name as output parameter
    os.system(f"echo 'TargetTableName={target_table_name}' >> $GITHUB_OUTPUT")
    
    backup_type = os.environ.get("DynamoDBRestoreMethod")

    s3_client = boto3.client('s3', region)

    # To deploy via manual snapshot
    if backup_type == 'Manual':
        backup_arn = os.environ.get("BackupArn")
        restored_table = dynamodb_client.restore_table_from_backup(
                                TargetTableName=target_table_name,
                                BackupArn=backup_arn
                        )
        
        logger.info(f"you have restored the dynamodb table from the specified backup, Name of the restored table is: {restored_table['TableDescription']['TableName']}")
        
        emailer.send_success_email(
            subject="Restore Succeeded!",
            content=f"New Restore DynamoDB table created named {target_table_name} from ARN: {backup_arn}"
        )
        
        # Validating from metadata
        metadata_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
        metadata_file_key = common.get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)

        if common.object_exists_in_s3(metadata_bucket_name, metadata_file_key, s3_client):
            fetch_and_validate_metadata(metadata_bucket_name, metadata_file_key, source_table_name, backup_arn, target_table_name, dynamodb_client, s3_client, logger, emailer)

        else:
            logger.info("No Metadata found")

    elif backup_type == 'ManualLatest':
        restore_from_latest_arn(dynamodb_client, region, logger, emailer)

    # To deploy via PITR method by specifying date and time
    elif backup_type == 'Pitrdate':

        restore_date_time = datetime.strptime(os.environ.get("PITRBackupDate"), "%Y-%m-%d::%H:%M")
        restored_table = dynamodb_client.restore_table_to_point_in_time(
                                SourceTableName=source_table_name,
                                TargetTableName=target_table_name,
                                RestoreDateTime=restore_date_time
                            )
        logger.info(f"you have restored the dynamodb table from PITR selected date, Name of the restored table is: {restored_table['TableDescription']['TableName']}")

        emailer.send_success_email(
            subject="Restore Succeeded!",
            content=f"New Restore DynamoDB table created named {target_table_name} from PITR Date: {restore_date_time}"
        )

    # To deploy via PITR with latest snapshot
    elif backup_type == 'Pitr':
        restored_table = dynamodb_client.restore_table_to_point_in_time(
                                SourceTableName=source_table_name,
                                TargetTableName=target_table_name,
                                UseLatestRestorableTime=True
                                )

        logger.info(f"you have restored the dynamodb table from PITR from the latest backup, Name of the restored table is: {restored_table['TableDescription']['TableName']}")

        emailer.send_success_email(
            subject="Restore Succeeded!",
            content=f"New Restore DynamoDB table created named {target_table_name} from Latest PITR"
        )
    else:
        logger.error("Please Specify valid Restore Method")
        emailer.send_failure_email(
            subject="Restore Failed!",
            content=f"New Restore DynamoDB table created named {target_table_name} from Latest PITR"
        )
        sys.exit(1)

except dynamodb_client.exceptions.TableAlreadyExistsException:
    error_message = 'Table with the same name already exist'
    logger.error(error_message)
    emailer.send_failure_email(
                    subject="Restore Failed!",
                    content=f"Restore of DynamoDB table failed\nReason: {error_message}"
                )
    sys.exit(1)

except dynamodb_client.exceptions.InvalidRestoreTimeException:
    error_message = 'No backup found for the specified date'
    logger.error(error_message)
    emailer.send_failure_email(
                    subject="Restore Failed!",
                    content=f"Restore of DynamoDB table failed\nReason: {error_message}"
                )
    sys.exit(1)
   
except dynamodb_client.exceptions.TableNotFoundException:
    error_message = 'No source table found with the specified name'
    logger.error(error_message)
    emailer.send_failure_email(
                    subject="Restore Failed!",
                    content=f"Restore of DynamoDB table failed\nReason: {error_message}"
                )
    sys.exit(1)

except Exception as e:
    ex_type, ex, tb = sys.exc_info()
    error_message = f"{ex_type}: {ex}"
    logger.error(error_message)
    
    emailer.send_failure_email(
                    subject="Restore Failed!",
                    content=f"Restore of DynamoDB table failed\nReason: {error_message}\nTraceback: {traceback.format_exc()}"
                )
    traceback.print_tb(tb)
    sys.exit(1)
