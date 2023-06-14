import boto3
import json, sys
from context import common

def create_metadata_before_backup(table_name, region):
    dynamodb_client = boto3.client('dynamodb', region)

    gsi_count, lsi_count, _ = common.get_table_info(table_name, dynamodb_client)
    item_count = common.get_item_count(table_name, dynamodb_client)  

    # Fetching table description #updated 
    response = dynamodb_client.describe_table(TableName=table_name)
    kms_key_arn = response['Table']['SSEDescription']['KMSMasterKeyArn'] if 'SSEDescription' in response['Table'] else 'No KMS Key'

    # Creating metadata dictionary
    metadata = {
        table_name: [
            {
                "item_count": item_count,
                "gsi_count": gsi_count,
                "lsi_count": lsi_count,
                "kms_key_arn": kms_key_arn,
            }
        ]
    }

    return metadata


def write_metadata_to_s3(metadata, table_name, region, s3_bucket_name, location, logger, emailer):
    """
    table_name: Name of DynamoDB Table which we are going to backup
    backup_name: Name of Backup that will be created

    This function will write/update json file in the provided s3 Bucket and location
    """
    try:
        # Connection to boto3 Clients
        s3_client = boto3.client('s3', region)
        
        # If the JSON file exists in given location
        if common.object_exists_in_s3(s3_bucket_name, location, s3_client):
            existing_metadata = common.read_json_from_s3(s3_bucket_name, location, s3_client)

            # If the key for operating table exists in dictionary, extend that dictionary
            if table_name in existing_metadata.keys():
                # Update Dictionary for that table
                existing_metadata[table_name].extend(metadata[table_name])
                metadata = existing_metadata

            else:
                # Add new Key to the Dictionary
                metadata.update(existing_metadata)

        # Write new/updated metadata Dictionary as JSON
        s3_client.put_object(
            Body=json.dumps(metadata, indent=4, sort_keys=True, default=str),
            Bucket=s3_bucket_name,
            Key=location
        )

        success_message = f"Metadata Created Successfully on s3 https://{s3_bucket_name}.s3.amazonaws.com/{location}"
        logger.info(success_message)
        return {
            "status": True,
            "message": success_message
        }
    
    except s3_client.exceptions.NoSuchBucket:
        error_message = "Bucket not Found!"
        logger.error(error_message)

        emailer.send_failure_email(
                    subject="Backup Failed!",
                    content=f"Backup of DynamoDB table {table_name} failed\nReason: {error_message}"
                )
        sys.exit(1)
