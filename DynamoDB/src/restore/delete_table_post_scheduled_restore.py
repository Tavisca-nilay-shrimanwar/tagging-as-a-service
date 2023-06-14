import boto3
import os, sys, traceback
from context import common, alerts

try:
    region = "us-east-1"

    # Initializing SSM Client
    s3_client = boto3.client('s3', region)
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

    target_table_name = os.environ.get("TargetTableName")


    dynamodb_client = boto3.client('dynamodb', region) 
    ddbresponse = dynamodb_client.delete_table(TableName=target_table_name)       
    print(f"Table {target_table_name} has been  deleted")

except (dynamodb_client.exceptions.TableNotFoundException, dynamodb_client.exceptions.ResourceNotFoundException):
    logger.error("Resoure Deletion Failed!")
    sys.exit(1)

except Exception as e:
    ex_type, ex, tb = sys.exc_info()
    error_message = f"Resoure Deletion Failed! {ex_type}: {ex}"
    logger.error(error_message)
    
    traceback.print_tb(tb)
    sys.exit(1)