import logging
import watchtower, boto3
import requests

#############################################################################
############################# LOGGER UTIL ####################################
#############################################################################

################## Requirements ######################################
# Need to provide a log_group name so that it gets created automatically.
# stream will be generated automatically by the library
# Below permissions are required for ec2 instance so that script send logs to the cloudwatch
# "logs:CreateLogGroup"
# "logs:CreateLogStream",
# "logs:PutLogEvents"

class Logger():
    def __init__(self, **kwargs) -> None:
        logging.basicConfig(level=logging.INFO)
        logger_name = kwargs["logger_name"]
        logger = logging.getLogger(logger_name)

        r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
        response_json = r.json()
        region = response_json.get('region')

        handler = watchtower.CloudWatchLogHandler(
            log_group = kwargs["log_group_name"],
            stream_name = "{logger_name}-{strftime:%Y-%m-%d} [{strftime:%H.%M UTC}]",
            create_log_group = True,
            boto3_client=boto3.client("logs", region_name=region)
        )

        logger.addHandler(handler)
        self.logger = logger

    def get_logger(self):
        return self.logger

################## Usage Example ######################################
# logger = Logger(
#      logger_name = "test",
#      log_group_name = "my-lg-grp"
# ).get_logger()
#
# logger.debug("Harmless debug Message")
# logger.info("Just an information")
# logger.warning("Its a Warning")
# logger.error("Did you try to divide by zero")
# logger.critical("Internet is down")