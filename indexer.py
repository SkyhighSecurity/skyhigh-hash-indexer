import yaml
import requests
from msal import ConfidentialClientApplication
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import csv

def load_config(filename):
    with open(filename, 'r') as file:
        return yaml.safe_load(file)

def connect_s3(access_key, secret_access_key):
    try:
        return boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key
        )
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error connecting to AWS: {e}")
        return None

def list_s3_buckets(s3, scan_all, specific_buckets):
    if scan_all:
        response = s3.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]
    else:
        return specific_buckets

def list_s3_objects(s3, buckets):
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

def write_s3_object_list(s3, bucket_name, file_key, data):
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

def authenticate_onedrive(account_info):
    try:
        client_id = account_info['client_id']
        client_secret = account_info['client_secret']
        tenant_id = account_info['tenant_id']
        authority_url = f"https://login.microsoftonline.com/{tenant_id}"
        app = ConfidentialClientApplication(
            client_id,
            authority=authority_url,
            client_credential=client_secret
        )
        token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if 'access_token' in token_response:
            return token_response['access_token']
        else:
            print(f"Token response did not contain access token: {token_response}")
            return None
    except Exception as e:
        print(f"Exception during authentication: {e}")
        return None

def get_onedrive_users_drive_items(graph_access_token):
    headers = {
        'Authorization': 'Bearer ' + graph_access_token,
        'Content-Type': 'application/json'
    }
    users_endpoint = "https://graph.microsoft.com/v1.0/users"
    users_response = requests.get(users_endpoint, headers=headers).json()
    
    object_details = []
    
    for user in users_response.get('value', []):
        drive_endpoint = f"https://graph.microsoft.com/v1.0/users/{user['id']}/drive/root/children"
        drive_response = requests.get(drive_endpoint, headers=headers).json()
        
        for item in drive_response.get('value', []):
            if 'file' in item:
                object_details.append({
                    'User': user['displayName'],
                    'File Name': item['name'],
                    'File Hash': item.get('file', {}).get('hashes', {}).get('quickXorHash', '')
                })

    return object_details

def write_onedrive_drive_items(data):
    with open('onedrive_files.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        for item in data:
            writer.writerow([item['User'], item['File Name'], item['File Hash']])

def process_accounts(config):
    all_objects = []
    output = config['outputs']['AWS']
    s3_output = connect_s3(output['accessKey'], output['secretAccessKey'])
    
    for account in config['inputs']['AWS']['awsAccounts']:
        s3 = connect_s3(account['accessKey'], account['secretAccessKey'])
        if s3:
            buckets = list_s3_buckets(s3, account['scanAllBuckets'], account['buckets'])
            objects = list_s3_objects(s3, buckets)
            all_objects.extend(objects)
    
    write_s3_object_list(s3_output, output['bucketName'], output['fileKey'], all_objects)

    for onedrive_account in config['inputs']['Microsoft']['onedriveAccounts']:
        graph_token = authenticate_onedrive(onedrive_account)
        if graph_token:
            items = get_onedrive_users_drive_items(graph_token)
            write_onedrive_drive_items(items)
        else:
            print(f"Failed to authenticate with Microsoft Graph API for OneDrive account: {onedrive_account}")

if __name__ == "__main__":
    config_path = 'config.yaml'
    config = load_config(config_path)
    # Clear existing content of onedrive_files.csv
    open('onedrive_files.csv', 'w').close()
    process_accounts(config)
