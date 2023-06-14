import json
import requests
from context import common

def take_immediate_backup(cluster_name, opscenter_ip, session_id, logger, keyspaces=[], destination="", compressed=True): #TODO: Make Destination to be passed as Mandatory
    #TODO: dont take destination, hardcode it from "CASSANDRA_CLUSTER_NAME_S3" ssm parameter. Bucket will be defined for each cluster at the time of onboarding.
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/run"
    headers = {
      'Content-Type': 'application/json',
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    # call API to get all the keyspace names
    all_keyspaces = list(common.get_all_keyspaces_for_cluster(cluster_name, opscenter_ip, session_id).keys()) if "all" in keyspaces else keyspaces
    if "OpsCenter" in all_keyspaces:
        all_keyspaces.remove("OpsCenter")

    # Check if any Keyspace is passed. If not, return None
    if len(keyspaces) == 0:
        logger.error(f"Please Provide one or more valid Keyspaces. pass 'all' in list to backup all keyspaces")
        return None

    # Else, Continue to later logic
    else:
        # If there is an 'all' keyword in the keyspace list, then backup for all keyspaces
        if "all" in keyspaces:
            pre_payload = {
              "keyspaces": all_keyspaces
            }

        # Else, backup only for passed keyspaces
        else:
            pre_payload = {
              "keyspaces": keyspaces
            }

        # If 'destination' parameter is set to non empty string, configure the destination in payload
        if destination != "":
            pre_payload = {**pre_payload, "destinations": {destination: {"compressed": compressed}}}
            destination_to_print = destination
        # Else, No need to confiure destination, as opscenter will backup to on_server by default
        else:
            destination_to_print = 'default'

        payload = json.dumps(pre_payload)

        print(f"Backup Process started for keyspaces: {','.join(all_keyspaces)} at destination: {destination_to_print}. Please Wait for Completion.")
        
        # Send Post Request to Opscenter API for Invoking Backup
        response = requests.request("POST", url, headers=headers, data=payload)
        parsed_response = json.loads(response.text)
        
        # If we get a dictionary in a response, it means that something went wrong. So we catch the error and print the message.
        if(type(parsed_response) is dict):
            print(parsed_response['message'])
        # Else, print the success message
        else:
            print(f"Backup created Successfully")

        return json.loads(response.text)


def delete_backup(cluster_name, opscenter_ip, session_id, backup_id, destination=None):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups?tag={backup_id}"

    if destination != None:
        url += f"&destination={destination}"

    payload={}
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    print(f"Deleting backup with tag: {backup_id}")

    # Send Delete Request to Opscenter API
    response = requests.request("DELETE", url, headers=headers, data=payload)
    return json.loads(response.text)


def create_immediate_bakup_after_configuring_destination(cluster_name, opscenter_ip, bucket_name, session_id, keyspaces, logger):
    import time

    # If Keyspaces Parameter is not passed then we backup all the keyspaces
    if keyspaces in [[], "", None, "all"]:
        keyspaces = ["all"]

    # Get all available destinations
    destinations = common.list_destinations(cluster_name, opscenter_ip, session_id) # Dictionary will all the destination details

    # Check if the destinaiton with our bucket exists
    destination_found = False
    destination_id = ""
    for destination in destinations.keys():
        if destinations[destination]["provider"] == bucket_name and destinations[destination]["path"] == 's3':
            destination_found = True
            destination_id = destination
            break

    if destination_found:
        # Backup to this destination
        print(f"Existing Destination found for bucket: {bucket_name}")
        take_immediate_backup(cluster_name, opscenter_ip, session_id, logger=logger, keyspaces=keyspaces, destination=destination_id, compressed=True)
    else:
        # Add Destination and then Backup
        response_after_adding_destination = common.add_destination(cluster_name, opscenter_ip, session_id, destination=bucket_name, logger=logger, destination_type='s3', server_side_encryption=True, acceleration_mode=False)
        print(f"Destination not found for bucket: {bucket_name}. Creating new one")
        destination_id = list(response_after_adding_destination['destination'][0].keys())[0]

        start_time = time.time()
        take_immediate_backup(cluster_name, opscenter_ip, session_id, logger=logger, keyspaces=keyspaces, destination=destination_id, compressed=True)
        end_time = time.time()
        print(f"Took {end_time-start_time}s to backup")

    # deleting the on-server backup
    last_backup_id = sorted([activity for activity in common.get_backup_activities(cluster_name, opscenter_ip, session_id, logger) if activity["type"] == "backup"], key=lambda x:x['event_time'], reverse=True)[0]["backup_id"]

    delete_backup(cluster_name, opscenter_ip, session_id, last_backup_id)
    print("Deleted the On Server Backup")