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


def write_metadata_to_s3(metadata, table_name, region, s3_bucket_name, location, logger, teams_messenger):
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

        success_title = "DynamoDB Metadata Created Successfully"
        success_message = f"Metadata Created Successfully for table {table_name} on s3 https://{s3_bucket_name}.s3.amazonaws.com/{location}"
        
        # Send Logs to CW
        logger.info(success_message)

        # Send Teams Message
        teams_messenger.send_message(success_title, success_message)
    
    except s3_client.exceptions.NoSuchBucket:
        error_title = "DynampDB Metadata creation failed"
        error_message = "Metadata creation failed for table {table_name}. Bucket with name: {s3_bucket_name} not Found!"
        
        logger.error(error_message)
        sys.exit(1)

        # Send Teams Message
        teams_messenger.send_message(error_title, error_message)

    except Exception as e:
        ex_type, ex, tb = sys.exc_info()

        error_title = "DynampDB Metadata creation failed"
        error_message = f"Metadata creation failed for table {table_name}. {ex_type}: {ex}"
        
        # Logging Failure
        logger.error(error_message)

        # Sending Teams Message
        teams_messenger.send_message(title=error_title, message=error_message)

        sys.exit(1)
