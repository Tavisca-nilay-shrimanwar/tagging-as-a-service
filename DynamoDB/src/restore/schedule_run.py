
import sys, traceback
import boto3
from context import common, alerts
from utils import restore_from_latest_arn

region = "us-east-1"

# Initializing SSM Client
ssm_client = boto3.client("ssm", region_name=region)
log_group_name = common.get_parameter_from_ssm("dynamodb-bnr-log-grp-name", ssm_client)

# Initiating logger
logger = alerts.Logger(
    logger_name = "schedule-restore",
    log_group_name = log_group_name
).get_logger()

alert_email_sender = common.get_parameter_from_ssm("bnr-alerts-sender-email", ssm_client)
alert_email_receiver = common.get_parameter_from_ssm("bnr-alerts-receiver-email-list", ssm_client)

emailer = alerts.Emailer(
    sender = alert_email_sender,
    receiver =alert_email_receiver.strip().split(",")
)

try:
    dynamodb_client = boto3.client('dynamodb', 'us-east-1')

    # Since this is scheduled, this will restore last successful manual Backup
    restore_from_latest_arn(dynamodb_client, region, logger, is_auto=True)

except dynamodb_client.exceptions.TableAlreadyExistsException:
    error_message = 'Table with the same name already exist'
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