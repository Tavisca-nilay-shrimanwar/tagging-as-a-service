import boto3
import requests

#############################################################################
############################# EMAIL UTIL ####################################
#############################################################################

################## Requirements ######################################
# Need to have a identity created in SES first before sending the email. 
# Identity can be either domain or email address
# AWS will send a confirmation email to the mail address
# We need to verify by clicking the link in the email.
# And then we can send the email using boto3.

class Emailer():
    CHARSET = "UTF-8"
     
    def __init__(self, **kwargs) -> None:

        r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
        response_json = r.json()
        region = response_json.get('region')

        self.client = boto3.client("ses", region_name=region)
        self.sender = kwargs["sender"]
        self.receiver = kwargs["receiver"]

    def send_success_email(self, **opts):
        content = self.get_success_template(opts["content"])
        
        try:
            return self.client.send_email(
                Destination={
                    "ToAddresses": self.receiver,
                },
                Message={
                    "Body": {
                            "Html": {
                                "Charset": self.CHARSET,
                                "Data": content,
                            }
                    },
                    "Subject": {
                            "Charset": self.CHARSET,
                            "Data": opts["subject"],
                    },
                },
                Source=self.sender
            )

        except self.client.exceptions.MessageRejected as e:
            print(f"Email is not Verified. Details: {e}")
            return None


    def send_failure_email(self, **opts):
        content = self.get_failure_template(opts["content"])
        
        try:
            return self.client.send_email(
                Destination={
                    "ToAddresses": self.receiver,
                },
                Message={
                    "Body": {
                            "Html": {
                                "Charset": self.CHARSET,
                                "Data": content,
                            }
                    },
                    "Subject": {
                            "Charset": self.CHARSET,
                            "Data": opts["subject"],
                    },
                },
                Source=self.sender
            )
        
        except self.client.exceptions.MessageRejected as e:
            print(f"Email is not Verified. Details: {e}")
            return None


    def get_success_template(self, content: str):
        return """
            <html>
                <head></head>
                <h3 style='color:green'>Success</h3>
                <p>{content}</p>
                </body>
            </html>
        """.format(content=content)

    def get_failure_template(self, content: str):
        return """
            <html>
                <head></head>
                <h3 style='color:red'>Failure</h3>
                <p>{content}</p>
                </body>
            </html>
        """.format(content=content)

################## Usage Example ######################################
# email = Email(
#     sender = "ganesh.lohar@tavisca.com",
#     receiver =["nilay.shrimanwar@tavisca.com"]
# )
# email.send_success_email(
#     subject="Backup success",
#     content="This is a success email details"
# )
# email.send_failure_email(
#     subject="Backup failed",
#     content="This is a failure email details"
# )