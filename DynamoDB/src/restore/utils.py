import json, time, os, sys, traceback
import boto3
from datetime import datetime
from context import common

def fetch_and_validate_metadata(metadata_bucket_name, metadata_file_key, source_table_name, backup_arn, target_table_name, dynamodb_client, s3_client, logger, emailer, teams_messenger):
    
    existing_metadata = common.read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client)
    
    # If the key for operating table exists in dictionary, extend that dictionary
    if source_table_name in existing_metadata.keys() and len(existing_metadata[source_table_name]) > 0:
        metadata_dict_for_arn = next(item for item in existing_metadata[source_table_name] if item["backup_arn"] == backup_arn)
        validate_metadata(target_table_name, dynamodb_client, metadata_dict_for_arn, logger, emailer, teams_messenger)
    
    else:
        print("No Existing Metadata found for selected Table")


def validate_metadata(target_table_name, dynamodb_client, metadata_dict, logger, emailer, teams_messenger):
    loop_count = 0
    while True:
        #restored_table = dynamodb_client.describe_table(TableName=target_table_name)['Table']
        gsi_count, lsi_count, table_status = common.get_table_info(target_table_name, dynamodb_client)

        #status = restored_table['TableStatus']
        if table_status in ['CREATING', 'UPDATING']:
            loop_count += 1
            logger.info(f"Iteration {loop_count}, Waiting for table to be created. Current status: {table_status}. Wait Time: {loop_count*30} Seconds")
            time.sleep(30) # CANDO:Multiply by wait count
        elif table_status == 'ACTIVE':
            gsi_count_in_restored_table = gsi_count
            lsi_count_in_restored_table = lsi_count
            break
        else:
            logger.error("Table Creation Failed")
            break

    
    #item_count_in_restored_table = dynamodb_client.scan(TableName=target_table_name, Select='COUNT')['Count']
    item_count_in_restored_table = common.get_item_count(target_table_name, dynamodb_client)  
    
    if metadata_dict["item_count"] == item_count_in_restored_table and metadata_dict["gsi_count"] == gsi_count_in_restored_table and metadata_dict["lsi_count"] == lsi_count_in_restored_table:
        success_title = "DynamoDB Table Created and Metadata Validation Succeded"
        success_message = f"Metadata Validation of DynamoDB table: {target_table_name} succeed"
        
        # Logging Successful Metadata Validation
        logger.info(success_message)
        
        # Sending Teams Message for Successful Metadata Validation
        teams_messenger.send_message(title=success_title, message=success_message)

        # Sending Email for Successful Metadata Validation
        emailer.send_success_email(
                    subject=success_title,
                    content=success_message
                )

    else:
        # Logging Failed Metadata Validation
        failure_title = "DynamoDB table created but Metadata Validation Failed!"
        logger.error(failure_title)

        if metadata_dict["item_count"] != item_count_in_restored_table:
            error_message = f"Item Count in Backup: {metadata_dict['item_count']}; Item count in Restored Table: {item_count_in_restored_table}"
            logger.error(error_message)
        if metadata_dict["gsi_count"] != gsi_count_in_restored_table:
            error_message = f"Item Count in Backup: {metadata_dict['gsi_count']}; Item count in Restored Table: {gsi_count_in_restored_table}"
            logger.error(error_message)
        else:
            error_message = f"Item Count in Backup: {metadata_dict['lsi_count']}; Item count in Restored Table: {lsi_count_in_restored_table}"
            logger.error(error_message)

        # Sending Teams Message for Successful Restore
        teams_messenger.send_message(failure_title, f"Restore Validation of DynamoDB table: {target_table_name} failed from latest ARN\nReason: {error_message}")

        # Sending Email for Failed Metadata Validation
        emailer.send_failure_email(
                    subject=failure_title,
                    content=f"Restore Validation of DynamoDB table: {target_table_name} failed from latest ARN\nReason: {error_message}"
                )


def restore_from_latest_arn(dynamodb_client, region, logger, emailer, teams_messenger, is_auto=False):
    s3_client = boto3.client('s3', region)
    ssm_client = boto3.client('ssm', region)

    metadata_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
    metadata_file_key = common.get_parameter_from_ssm("dynamodb-metadata-file-key", ssm_client)

    source_table_name = os.environ.get("TableNameForBackup")
    datetime_str = datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
    target_table_name = "auto_restored_"+source_table_name+"_"+datetime_str if is_auto else "restored_"+source_table_name+"_"+datetime_str

    # Setting target table name as output parameter
    os.system(f"echo 'TargetTableName={target_table_name}' >> $GITHUB_OUTPUT")

    if common.object_exists_in_s3(metadata_bucket_name, metadata_file_key, s3_client):
            existing_metadata = common.read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client)

            # If the key for operating table exists in dictionary, extend that dictionary
            if source_table_name in existing_metadata.keys():
                # Taking latest Restore ARN
                latest_backup_metadata_dict = sorted(existing_metadata[source_table_name], key=lambda _: _['timestamp'], reverse=True)[0]
                latest_backup_arn = latest_backup_metadata_dict["backup_arn"]

                # Deploying via manual snapshot
                restored_table = dynamodb_client.restore_table_from_backup(
                                        TargetTableName=target_table_name,
                                        BackupArn=latest_backup_arn
                                    )
                
                success_title = "DynamoDB Restore operation Started"
                success_message = f"DynamoDB Restore process started from latest backup ARN: {latest_backup_arn}, Name of the restored table is: {restored_table['TableDescription']['TableName']}"
                
                # Logging Successful Restore
                logger.info(success_message)

                # Sending Teams Message for Successful Restore
                teams_messenger.send_message(title=success_title, message=success_message)

                # Sending Email for Successful Restore
                emailer.send_success_email(
                    subject=success_title,
                    content=success_message
                )

                # Validation
                validate_metadata(target_table_name, dynamodb_client, latest_backup_metadata_dict, logger, emailer, teams_messenger)

            else:
                failure_title =  "DynamoDB Restore Operation Failed"
                failure_message = f"Restore of DynamoDB table failed from latest ARN\nReason: {failure_message}"
        
                # Logging Failed Restore
                logger.error(failure_message)
                
                # Sending Teams Message for Failed Restore
                teams_messenger.send_message(title=failure_title, message=failure_message)

                # Sending Email for Failed Restore
                emailer.send_failure_email(
                    subject=failure_title,
                    content=failure_message
                )
                sys.exit(1)
    else:
        failure_title = "DynamoDB Restore Operation Failed"
        failure_message = "No Metadata found"

        # Logging Failed Restore
        logger.error(failure_message)

        # Sending Teams Message for Failed Restore
        teams_messenger.send_message(title=failure_title, message=failure_message)

        # Sending Email for Failed Restore
        emailer.send_failure_email(
                    subject="Restore Failed!",
                    content=f"Restore of DynamoDB table failed from latest ARN\nReason: {failure_message}"
                )
        
        sys.exit(1)


def write_retention_config(table_name, rentntion_period_days, ssm_client, s3_client, logger, emailer):
    
    try:
        retention_config_bucket_name = common.get_parameter_from_ssm("dynamodb-metadata-bucket-name", ssm_client)
        retention_config_file_key = common.get_parameter_from_ssm("dynamodb-config-file-key", ssm_client)

        retention_config = {
            table_name: {
                "creation_date_time": datetime.now().strftime('%d-%m-%y %H:%M:%S'),
                "days_to_retain": rentntion_period_days
            }
        }

        if common.object_exists_in_s3(retention_config_bucket_name, retention_config_file_key, s3_client):
            existing_config = common.read_json_from_s3(retention_config_bucket_name, retention_config_file_key, s3_client)
            retention_config.update(existing_config)

        # Write new/updated config Dictionary as JSON
        s3_client.put_object(
            Body=json.dumps(retention_config, indent=4, sort_keys=True, default=str),
            Bucket=retention_config_bucket_name,
            Key=retention_config_file_key
        )

        logger.info("Retention config written Successfully!")

    except s3_client.exceptions.NoSuchBucket:
        error_message = "Bucket not Found!"
        logger.error(error_message)
        
        return {
            "status": False,
            "message": error_message
        }

    except Exception as e:
        ex_type, ex, tb = sys.exc_info()
        failure_message = f"{ex_type}: {ex}"
        logger.error(failure_message)
        traceback.print_tb(tb)
        sys.exit(1)
