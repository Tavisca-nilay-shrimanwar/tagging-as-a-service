import boto3
import os, sys, traceback
from datetime import datetime
from context import common, alerts

region = 'us-east-1'
s3_client = boto3.client('s3', region)
ssm_client = boto3.client('ssm', region)

log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

logger = alerts.Logger(
     logger_name = "list-backups",
     log_group_name = log_group_name
).get_logger()


try:

    table_name = os.environ.get("TableNameForBackup")
    metadata_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
    metadata_file_key = common.get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)

    backup_dict = common.read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client)

    if table_name in backup_dict.keys() and len(backup_dict[table_name]) > 0:

        dynamodb_client = boto3.client('dynamodb', 'us-east-1')

        actual_backups = dynamodb_client.list_backups(TableName=table_name)
        actual_arns = [backup['BackupArn'] for backup in actual_backups['BackupSummaries']]

        logger.info(f"Listing Available Backups for table {table_name}")

        # Print the names of the columns.
        print("{:<10} {:<10} {:<10} {:<25} {:<25}".format('ITEM COUNT', 'GSI COUNT', 'LSI COUNT', 'TIMESTAMP', 'BACKUP ARN'))
        print('-'*150)

        for backup_info in backup_dict[table_name]:
            print('-'*150)
            
            # print each data item.
            key, value = zip(*backup_info.items())
            backup_arn, backup_retention_days, gsi_count, item_count, kms_key_arn, lsi_count, timestamp = value
            if backup_arn in actual_arns:
                #logger.info("\n{:<10} {:<10} {:<10} {:<20} {:<10}".format(item_count, gsi_count, lsi_count, timestamp, backup_arn))
                print("{:<10} {:<10} {:<10} {:<25} {:<25}".format(item_count, gsi_count, lsi_count, datetime.fromtimestamp(timestamp).strftime('%d-%m-%y %H:%M:%S'), backup_arn))

    else:
        print("No Existing Metadata found for selected Table")
        sys.exit(1)


except Exception as e:
    ex_type, ex, tb = sys.exc_info()
    error_message = f"{ex_type}: {ex}"
    logger.error(error_message)
    traceback.print_tb(tb)
    sys.exit(1)
