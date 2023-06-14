import requests
import json, sys, traceback
from context import common
from datetime import datetime

def restore_all_keyspace_from_backup(cluster_name, opscenter_ip, session_id, backup_tag, destination_id):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/restore/{backup_tag}"

    payload={
        "destination": destination_id
     }
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    print(f"Restoring backup with tag: {backup_tag}")
    # Send Post Request to Opscenter API
    response = requests.request("POST", url, headers=headers, data=payload)

    return json.loads(response.text)


def restore_keyspaces_from_backup(cluster_name, opscenter_ip, session_id, backup_id, destination_id, logger, keyspaces=[], restore_all=True):

    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/restore/{backup_id}"
    
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    # call API to get all the keyspace names
    all_keyspaces = list(common.get_all_keyspaces_for_cluster(cluster_name, opscenter_ip, session_id).keys()) if "All" in keyspaces else keyspaces

    if "OpsCenter" in all_keyspaces:
        all_keyspaces.remove("OpsCenter")

    if restore_all:
        keyspaces_to_restore = dict.fromkeys(all_keyspaces, {})
    else:
        passed_keyspaces = [keyspace for keyspace in keyspaces if keyspace in all_keyspaces]
        keyspaces_to_restore = dict.fromkeys(passed_keyspaces, {})

    payload={
        "destination": destination_id,
        "keyspace": keyspaces_to_restore
    }

    print(f"Restoring backup with tag: {backup_id}")

    # Send Post Request to Opscenter API
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

    parsed_response = json.loads(response.text)
    print(parsed_response)

    if isinstance(parsed_response, dict):
        error_message = parsed_response['message']
        logger.error(error_message)
        sys.exit(1)

    print(f"Restore started with session: {parsed_response}")
    return parsed_response


def restore_specific_table_from_backup(cluster_name, opscenter_ip, session_id, backup_id, destination_id, keyspace, table, logger):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/restore/{backup_id}/{keyspace}/{table}"

    payload={
        "destination": destination_id
    }
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    print(f"Restoring table: {table} in keyspace: {keyspace} using backup with tag: {backup_id}")
    # Send Post Request to Opscenter API
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
    parsed_response = json.loads(response.text)

    if isinstance(parsed_response, dict):
        error_message = parsed_response['message']
        logger.error(error_message)
        sys.exit(1)


    return parsed_response


def restore_based_on_scope(cluster_name, opscenter_ip, session_id, backup_id, destination_id, restore_scope, keyspaces, logger, table):
    if restore_scope == 'All':
        restore_keyspaces_from_backup(cluster_name, opscenter_ip, session_id, backup_id, destination_id, logger, keyspaces=["All"])
    elif restore_scope == 'Keyspace':
        restore_keyspaces_from_backup(cluster_name, opscenter_ip, session_id, backup_id, destination_id, logger, keyspaces=keyspaces, restore_all=False)
    elif restore_scope == 'Table':
        restore_specific_table_from_backup(cluster_name, opscenter_ip, session_id, backup_id, destination_id, keyspaces[0], table, logger)
    else:
        # TODO: Raise exception to pass valid restore_type
        pass


def restore_after_configuring_destination(cluster_name, opscenter_ip, session_id, bucket_name, keyspaces, backup_id, logger, restore_scope='all', table=None):
    '''
    Args:
    session_id: string
    keyspaces: list of strings
    restore_scope: string; Options are: 'All', 'Keyspace', 'Table'
    table: string; For the scenario when restore_scope is 'Table'
    '''
    # Get all available destinations
    destinations = common.list_destinations(cluster_name, opscenter_ip, session_id) # Dictionary will all the destination details

    # Check if the destinaiton with out bucket exists
    destination_found = False
    destination_id = ""
    for destination in destinations.keys():
        if destinations[destination]["path"] == bucket_name and destinations[destination]["provider"] == 's3':
            destination_found = True
            destination_id = destination
            break

    if destination_found:
        # Restore using this destination
        print(f"Existing Destination found for bucket: {bucket_name}")
        restore_based_on_scope(cluster_name, opscenter_ip, session_id, backup_id, destination_id, restore_scope, keyspaces, logger, table)
    else:
        # Add Destination and then restore
        print(f"Destination not found for bucket: {bucket_name}. Creating new one")
        response_after_adding_destination = common.add_destination(cluster_name, opscenter_ip, session_id, destination=bucket_name, logger=logger, destination_type='s3', server_side_encryption=True, acceleration_mode=True)
        destination_id = list(response_after_adding_destination['destination'][0].keys())[0]
        restore_based_on_scope(cluster_name, opscenter_ip, session_id, backup_id, destination_id, restore_scope, keyspaces, logger, table)


def write_retention_config(cluster_name, rentntion_period_days, ssm_client, s3_client, logger, emailer):
    
    try:
        retention_config_bucket_name = common.get_parameter_from_ssm("cassandra-metadata-bucket-name", ssm_client)
        retention_config_file_key = common.get_parameter_from_ssm("cassandra-config-file-key", ssm_client)

        retention_config = {
            cluster_name: {
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


