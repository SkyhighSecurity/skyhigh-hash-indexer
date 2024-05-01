import yaml
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import csv


def load_config(filename):
    with open(filename, 'r') as file:
        return yaml.safe_load(file)

def connect_to_s3(access_key, secret_access_key):
    try:
        return boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error connecting to AWS: {e}")
        return None

def list_or_specific_buckets(s3, scan_all, specific_buckets):
    if scan_all:
        response = s3.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]
    else:
        return specific_buckets

def gather_bucket_objects(s3, buckets):
    object_details = []
    for bucket in buckets:
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    cleaned_etag = obj['ETag'].replace('"', '').replace('\\', '')  # Remove quotes and slashes
                    object_details.append({
                        'Bucket': bucket,
                        'Key': obj['Key'],
                        'ETag': cleaned_etag
                    })
        except Exception as e:
            print(f"Error accessing bucket {bucket}: {e}")
    return object_details


def write_to_output_bucket(s3, bucket_name, file_key, data):
    try:
        # Prepare CSV data
        csv_content = "ETag,Bucket,Key\n"
        for item in data:
            csv_content += f"{item['ETag']},{item['Bucket']},{item['Key']}\n"

        # Write CSV data to the output bucket
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content.encode())
        print(f"Data successfully written to {bucket_name}/{file_key}")
    except Exception as e:
        print(f"Error writing to bucket: {e}")

def process_accounts(config):
    all_objects = []
    output = config['outputs']['AWS']
    s3_output = connect_to_s3(output['accessKey'], output['secretAccessKey'])
    
    for account in config['inputs']['AWS']['awsAccounts']:
        s3 = connect_to_s3(account['accessKey'], account['secretAccessKey'])
        if s3:
            buckets = list_or_specific_buckets(s3, account['scanAllBuckets'], account['buckets'])
            objects = gather_bucket_objects(s3, buckets)
            all_objects.extend(objects)
    
    write_to_output_bucket(s3_output, output['bucketName'], output['fileKey'], all_objects)

if __name__ == "__main__":
    config_path = 'config.yaml'
    config = load_config(config_path)
    process_accounts(config)
