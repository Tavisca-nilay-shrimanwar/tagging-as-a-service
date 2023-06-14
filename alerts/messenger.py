import boto3
import requests, json
from datetime import datetime

class TeamsMessenger():
    
     
    def __init__(self, webhook_list, log_grp_name, **kwargs) -> None:

        r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
        response_json = r.json()
        self.region = response_json.get('region')

        if len(webhook_list) == 0:
            print("INFO: Unable to initiate Teams messanger. Reason: No Webhook URL provided")
            return None

        self.webhook_list = webhook_list
        self.log_grp_name = log_grp_name


    def send_message(self, title, message):
        cloud_watch_url = f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}#logsV2:log-groups/log-group/{self.log_grp_name}"
        
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "activitySubtitle": datetime.now().strftime("%m-%d-%Y %H:%M:%S"),
                "text": message,
                "potentialAction": [
                    {
                        "@type": "OpenUri",
                        "name": "View CloudWatch Logs",
                        "targets": [
                            {
                                "os": "default",
                                "uri": cloud_watch_url
                            }
                        ]
                    }
                ]
            }]
        }

        # Sending message to all the channels
        for webhook_url in self.webhook_list:

            response = requests.post(
                url=webhook_url, 
                headers=headers, 
                data=json.dumps(payload)
            )

            if response.status_code != 200:
                print(f"Request to Teams channel with webhook{webhook_url} returned an error {response.status_code}, the response is:\n{response.text}")
                continue

