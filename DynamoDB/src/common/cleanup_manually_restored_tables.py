import boto3
import sys, traceback
from datetime import datetime, timedelta
from common_utils import get_parameter_from_ssm, read_json_from_s3
from context import alerts 

region = "us-east-1"

try:
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

    retention_config_bucket_name = get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
    retention_config_file_key = get_parameter_from_ssm("dynamodb-config-file-key", ssm_client)

    retention_config = read_json_from_s3(retention_config_bucket_name, retention_config_file_key, s3_client)

    dynamodb_client = boto3.client('dynamodb', region) 

    for table_name in retention_config:

        # Skip Deletion if retention days are set to 0
        if retention_config[table_name]["days_to_retain"] == "0":
            continue

        format = "%d-%m-%y %H:%M:%S"
        creation_datetime = datetime.strptime(retention_config[table_name]["creation_date_time"], format)
        days_to_retain = int(retention_config[table_name]["days_to_retain"])

        try:
            expiration_date = creation_datetime + timedelta(days=days_to_retain)
            if datetime.now() > expiration_date:
                ddbresponse = dynamodb_client.delete_table(TableName=table_name)
                logger.info(f"Table {table_name} is Expired, and hence Deleted!")
        
        except (dynamodb_client.exceptions.TableNotFoundException, dynamodb_client.exceptions.ResourceNotFoundException):
            logger.error(f"Table with name {table_name} was Deleted Before Expiration.")
            #TODO: We should remove this corrupt entry from the retention config file as it will cause unnecessary API calls
            continue

except Exception as e:
    ex_type, ex, tb = sys.exc_info()
    error_message = f"Resoure Deletion Failed! {ex_type}: {ex}"
    logger.error(error_message)
    
    traceback.print_tb(tb)
    sys.exit(1)