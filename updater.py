import boto3
import os
import json

bucket = os.environ["BUCKET"]
sid = os.environ["SID"]

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

client = boto3.client("s3")

try:
    client.put_object(
        Bucket = bucket,
        Key = f"{sid}@cxloyaltycorp.com/tags.json",
        Body = (bytes(json.dumps(tags_json).encode('UTF-8')))
    )
except BaseException as e:
    raise(e)
else:
    print(f"Tags updated successfully for SID - {sid}")
