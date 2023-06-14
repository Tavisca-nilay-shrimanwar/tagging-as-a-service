import boto3
import os
from utils import write_retention_config
from context import common, alerts

region = "us-east-1"
ssm_client = boto3.client("ssm", region_name=region)
s3_client = boto3.client('s3', region)

log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "write-retention-config",
    log_group_name = log_group_name
).get_logger()

alert_email_sender = common.get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
alert_email_receiver = common.get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

emailer = alerts.Emailer(
    sender = alert_email_sender,
    receiver =alert_email_receiver.strip().split(",")
)

target_table_name = os.environ.get("TargetTableName")
retention_period = os.environ.get("TableRetention")

write_retention_config(target_table_name, retention_period, ssm_client, s3_client, logger, emailer)



