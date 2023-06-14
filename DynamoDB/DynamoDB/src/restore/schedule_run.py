
import sys, traceback, os
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

teams_messenger = alerts.TeamsMessenger(webhook_list=["https://trilegiant.webhook.office.com/webhookb2/24b3f38b-6a41-4216-b747-d7693fc34b46@be80116c-1704-4639-8c7f-77ded4343d23/IncomingWebhook/fdfe1ef9ebe549fabc660d7e1f189e33/ce7ec5b7-b9c4-47d6-a605-61bcd8f1cb11"], log_grp_name=log_group_name)


try:
    dynamodb_client = boto3.client('dynamodb', 'us-east-1')

    # Since this is scheduled, this will restore last successful manual Backup
    restore_from_latest_arn(dynamodb_client, region, logger, emailer, teams_messenger, is_auto=True)


except dynamodb_client.exceptions.TableAlreadyExistsException:
    error_title = f"DynamoDB Restore Failed for table: {os.environ.get('TableNameForBackup')}"
    error_message = f"Restore of DynamoDB table failed\nReason: Table with the same name already exist"
    logger.error(error_message)
    emailer.send_failure_email(
                    subject=error_title,
                    content=error_message
                )
    teams_messenger.send_message(title=error_title, message=error_message)

    sys.exit(1)

except dynamodb_client.exceptions.TableNotFoundException:
    error_title = f"DynamoDB Restore Failed for table: {os.environ.get('TableNameForBackup')}"
    error_message = 'Restore of DynamoDB table failed\nReason: No source table found with the specified name'
    logger.error(error_message)
    emailer.send_failure_email(
                    subject=error_title,
                    content=error_message
                )
    teams_messenger.send_message(title=error_title, message=error_message)

    sys.exit(1)

except Exception as e:
    error_title = f"DynamoDB Restore Failed for table: {os.environ.get('TableNameForBackup')}"

    ex_type, ex, tb = sys.exc_info()
    error_message  = f"Restore of DynamoDB table failed\nReason: {ex_type}: {ex}\nTraceback: {traceback.format_exc()}"
    
    logger.error(error_message)
    emailer.send_failure_email(
                    subject=error_title,
                    content=error_message
                )
    traceback.print_tb(tb)
    sys.exit(1)