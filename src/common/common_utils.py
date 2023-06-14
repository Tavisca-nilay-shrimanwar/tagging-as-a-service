import boto3
import json

def get_parameter_from_ssm(parameter_name, ssm_client, with_decryption=False):
  """
  parameter_name: Name of parameter to get
  ssm_client: boto3 SSM Client Object
  """

  parameter = ssm_client.get_parameter(Name=parameter_name, WithDecryption=with_decryption)
  return str(parameter['Parameter']['Value']).strip()

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
    
def get_table_info(table_name, dynamodb_client):
  """returns gsi_count, lsi_count, table_status"""

  # Fetching Table information
  response = dynamodb_client.describe_table(TableName=table_name)

  gsi_count = len(response['Table']['GlobalSecondaryIndexes']) if 'GlobalSecondaryIndexes' in response['Table'].keys() else 0
  lsi_count = len(response['Table']['LocalSecondaryIndexes']) if 'LocalSecondaryIndexes' in response['Table'].keys() else 0
  status = response['Table']['TableStatus']

  return gsi_count, lsi_count, status

def get_item_count(table_name, dynamodb_client):
  scan = dynamodb_client.scan(TableName=table_name, Select='COUNT')
  count = scan["Count"]

  while 'LastEvaluatedKey' in scan:
    scan = dynamodb_client.scan(TableName=table_name, ExclusiveStartKey=scan['LastEvaluatedKey'], Select='COUNT')
    count = count + scan["Count"]

  return count

def read_json_from_s3(metadata_bucket_name, metadata_file_key, s3_client):
  existing_metadata_file = s3_client.get_object(Bucket=metadata_bucket_name, Key=metadata_file_key)
  existing_metadata = json.loads(existing_metadata_file["Body"].read().decode())
  
  return existing_metadata