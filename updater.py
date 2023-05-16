import boto3
import os
import json
import sys

bucket = os.environ["BUCKET"]
sid = os.environ["SID"]
client = boto3.client("s3")

tags_json = {
    "Name": os.environ["NAME"],
    "AppName": os.environ["APP_NAME"],
    "BusinessUnit": os.environ["BUSINESS_UNIT"],
    "Product": os.environ["PRODUCT"],
    "Backup": os.environ["BACKUP"],
    "Environment": os.environ["ENVIRONMENT"],
    "InfraOwner": os.environ["INFRA_OWNER"],
    "ProductOwner": os.environ["PRODUCT_OWNER"]
}

def update_tag():
    try:
        client.put_object(
            Bucket = bucket,
            Key = f"{sid}@cxloyaltycorp.com/tags.json",
            Body = (bytes(json.dumps(tags_json).encode('UTF-8')))
        )
    except BaseException as e:
        print(e)
        sys.exit(1)
    else:
        print(f"Tags updated successfully for SID - {sid}")

update_tag()