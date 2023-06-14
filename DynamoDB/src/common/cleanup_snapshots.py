import boto3
import sys, traceback
from datetime import datetime, timedelta
from common_utils import get_parameter_from_ssm, read_json_from_s3, get_region
from context import alerts


try:
    region = get_region()

    # Initializing SSM Client
    s3_client = boto3.client('s3', region)
    ssm_client = boto3.client("ssm", region_name=region)
    log_group_name = get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

    # Initiating logger
    logger = alerts.Logger(
        logger_name = "mannual-restore",
        log_group_name = log_group_name
    ).get_logger()

    alert_email_sender = get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
    alert_email_receiver = get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

    emailer = alerts.Emailer(
        sender = alert_email_sender,
        receiver =alert_email_receiver.strip().split(",")
    )

    metadata_bucket_name = get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
    metadata_file_key = get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)

    existing_metadata = read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client)

    dynamodb_client = boto3.client('dynamodb', region) 

    for table_name in existing_metadata:
        for metadata in existing_metadata[table_name]:

            try:
                arn_to_delete = metadata["backup_arn"]
                creation_datetime = datetime.fromtimestamp(metadata["timestamp"])
                days_to_retain = int(metadata["backup_retention_days"])

                expiration_date = creation_datetime + timedelta(days=days_to_retain)
                print(arn_to_delete, expiration_date)
                if datetime.now() > expiration_date:
                    ddbresponse = dynamodb_client.delete_backup(
                                    BackupArn=arn_to_delete
                                )
                    logger.info(f"Backup with ARN {arn_to_delete} for table {table_name} is Expired, and hence Deleted!")
            
            except (dynamodb_client.exceptions.BackupNotFoundException, dynamodb_client.exceptions.ResourceNotFoundException):
                logger.error(f"Backup with arn {arn_to_delete} for {table_name} was Deleted Before Expiration.")
                #TODO: We should remove this corrupt entry from the retention config file as it will cause unnecessary API calls
                continue

            except KeyError as e:
                logger.error(f"Key: {e} missing for Backup with arn {arn_to_delete} for {table_name}.")
                continue

            except dynamodb_client.exceptions.BackupInUseException:
                logger.error(f"Backup with arn {arn_to_delete} for {table_name} is already in use, hence cannot be deleted now, please try doint that later.")
                continue

except Exception as e:
    ex_type, ex, tb = sys.exc_info()
    error_message = f"Resoure Deletion Failed! {ex_type}: {ex}"
    logger.error(error_message)
    
    traceback.print_tb(tb)
    sys.exit(1)