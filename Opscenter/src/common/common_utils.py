import requests, boto3
import json, sys, urllib

def get_parameter_from_ssm(parameter_name, ssm_client, with_decryption=False):
  """
  parameter_name: Name of parameter to get
  ssm_client: boto3 SSM Client Object
  """

  parameter = ssm_client.get_parameter(Name=parameter_name, WithDecryption=with_decryption)
  return str(parameter['Parameter']['Value']).strip()


def authenticate(opscenter_ip, logger):
  '''Returns session Id which can be used to pass in other methods'''
  # Prepare Request data
  url = f"http://{opscenter_ip}/login"

  ssm_client = boto3.client('ssm', region_name='us-east-1')
  payload = json.dumps({
    "username": ssm_client.get_parameter(Name="opscenter-poc-user-id")['Parameter']['Value'],
    "password": ssm_client.get_parameter(Name="opscenter-poc-password", WithDecryption=True)['Parameter']['Value']
  })
  headers = {
    'Content-Type': 'application/json'
  }

  # Send Get Request to Opscenter API
  response = requests.request("POST", url, headers=headers, data=payload)
  parsed_response = json.loads(response.text)
      
  if 'message' in parsed_response:
      logger.error("Failed to authenticate to Opscenter")
      sys.exit(1)
  else:
      return json.loads(response.text)


def get_backup_activities(cluster_name, opscenter_ip, session_id, logger, params=None):
    '''Args: Sessionf_id: string
    Returns list of all backup activities'''
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backup-activity"

    if params != None:
      url = f"{url}?{urllib.parse.urlencode(params)}"


    payload = ""
    headers = {
        "opscenter-session": session_id
    }

    # Send Get Request to Opscenter API
    response = requests.request("GET", url, headers=headers, data=payload)
    return json.loads(response.text)


def get_backups(cluster_name, opscenter_ip, session_id):
    '''Args: Sessionf_id: string
    Returns list of all backup activities'''
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups"

    payload={}
    headers = {
        "opscenter-session": session_id
    }

    # Send Get Request to Opscenter API
    response = requests.request("GET", url, headers=headers, data=payload)

    return json.loads(response.text)


def add_destination(cluster_name, opscenter_ip, session_id, destination, logger, destination_type='s3', server_side_encryption=True, acceleration_mode=True):
    ''''''

    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/destinations"

    #Add destination details
    payload = json.dumps({
      "delete_this": False,
      "provider": destination_type,
      "path": destination,
      "region": "us-east-1", # TODO: Take region from param
      "access_key": "",
      "access_secret": "",
      "server_side_encryption": server_side_encryption,
      "acceleration_mode": acceleration_mode
    })
    headers = {
      'Content-Type': 'application/json',
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    print(f"Adding {destination_type} Destination: {destination} with: Server Side Encryption as: {server_side_encryption} and Acceleration Mode as: {acceleration_mode}")
    # Send Post Request to Opscenter API
    response = requests.request("POST", url, headers=headers, data=payload)
    parsed_response = json.loads(response.text)

    #If correct destination is passed, print the success message
    if 'destination' in parsed_response and len(parsed_response['destination']) > 0:
        print(f"Added Destination with id: {list(parsed_response['destination'][0].keys())[0]}")
    #Else, print error message
    else:
        logger.error(f"Failed to add Destination: {parsed_response['message']}")
        print()

    return parsed_response


def list_destinations(cluster_name, opscenter_ip, session_id):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/destinations"

    payload={}
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }
    # Send Get Request to Opscenter API
    response = requests.request("GET", url, headers=headers, data=payload)

    return json.loads(response.text)


def get_specific_destination(cluster_name, opscenter_ip, session_id, destination_id):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/backups/destinations/{destination_id}"
    payload={}
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }
    # Send Get Request to Opscenter API
    response = requests.request("GET", url, headers=headers, data=payload)
    parsed_response = json.loads(response.text)
    #If path is in response, it means the request suceeded thus returning the response
    if 'path' in parsed_response:
        return parsed_response
    #Else print the error message
    else:
        print(parsed_response['message'])
        return None


def delete_destination(cluster_name, opscenter_ip, session_id, destination_id):

    # Check if destination_id is valid
    if get_specific_destination(session_id, destination_id) == None:
        print("Please enter Valid Destination Id")
        return None

    # Else, Proceed to Delete the Destination
    url = f"http://{opscenter_ip}/{cluster_name}/backups/destinations/{destination_id}"
    payload={}
    headers = {
      'Cookie': f'TWISTED_SESSION={session_id}'
    }

    # Send Delete Request to Opscenter API
    response = requests.request("DELETE", url, headers=headers, data=payload)
    return json.loads(response.text)


def get_all_keyspaces_for_cluster(cluster_name, opscenter_ip, session_id):
    # Prepare Request data
    url = f"http://{opscenter_ip}/{cluster_name}/keyspaces"

    payload={}
    headers = {
        "opscenter-session": session_id
    }

    # Send Get Request to Opscenter API
    response = requests.request("GET", url, headers=headers, data=payload)

    return json.loads(response.text)

def get_backups_on_s3(cluster_name, opscenter_ip, session_id, logger):
    filter_params = {
      "filter_type": "backup",
      "filter_live": 1,
      "filter_success": 1
    }

    activities = get_backup_activities(cluster_name, opscenter_ip, session_id, logger, filter_params)
    backups_on_s3 = []

    for activity in activities:
        if activity['deleted_at'] == None and 'provider' in activity['destination'] and activity['destination']['provider'] == 's3':
            backups_on_s3.append(activity)

    return backups_on_s3

def get_region():
  r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
  response_json = r.json()
  return response_json.get('region')


def object_exists_in_s3(s3_bucket, s3_key, s3_client):
  """
  s3_bucket: name of the s3 bucket in which to check
  s3_key: loaction of the file
  s3_client: boto3 S3 Client Object

  Returns Boolean
  """

  response = s3_client.list_objects(
    Bucket = s3_bucket,
    Prefix = s3_key
    )
  if 'ETag' in str(response):
    return True
  else:
    return False


def read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client):
  json_file = s3_client.get_object(Bucket=metadata_bucket_name, Key=metadata_file_key)
  parsed_data = json.loads(json_file["Body"].read().decode())
  
  return parsed_data
