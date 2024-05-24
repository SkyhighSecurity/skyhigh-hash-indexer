import yaml
import requests
import hashlib
from msal import ConfidentialClientApplication
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import sqlite3
import csv
import os

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

def download_db_file(s3, bucket_name, db_file_key, local_db_file):
    try:
        s3.download_file(bucket_name, db_file_key, local_db_file)
        print(f"Database file {db_file_key} downloaded from S3 bucket {bucket_name}")
        return True
    except s3.exceptions.NoSuchKey:
        print(f"Database file {db_file_key} not found in S3 bucket {bucket_name}. A new database will be created.")
        return False
    except Exception as e:
        print(f"Error downloading database file from S3: {e}")
        return False

def upload_db_file(s3, bucket_name, db_file_key, local_db_file):
    try:
        s3.upload_file(local_db_file, bucket_name, db_file_key)
        print(f"Database file {local_db_file} uploaded to S3 bucket {bucket_name}/{db_file_key}")
    except Exception as e:
        print(f"Error uploading database file to S3: {e}")

def list_s3_buckets(s3, scan_all, specific_buckets, account_id):
    if scan_all:
        response = s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        for bucket_name in bucket_names:
            print(f"{account_id}\\{bucket_name}")
        return bucket_names
    else:
        for bucket_name in specific_buckets:
            print(f"{account_id}\\{bucket_name}")
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

def write_s3_object_list(s3, bucket_name, file_key, data, retain_files):
    try:
        # Prepare CSV data
        csv_content = "ETag,Bucket,Key\n"
        local_file_path = None
        for item in data:
            csv_content += f"{item['ETag']},{item['Bucket']},{item['Key']}\n"

        # Write CSV data to the output bucket
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_content.encode())
        print(f"Data successfully written to {bucket_name}/{file_key}")
        
        # Write a local copy if retainFiles is true
        if retain_files:
            local_file_path = file_key.split('/')[-1]
            with open(local_file_path, 'w') as local_file:
                local_file.write(csv_content)
            print(f"Local copy of the CSV file written to {local_file_path}")
            
        return local_file_path
        
    except Exception as e:
        print(f"Error writing to bucket: {e}")
        return None

def write_onedrive_index_file(data, file_path):
    try:
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["User Email", "File Name", "MD5"])
            for item in data:
                writer.writerow([item['User'], item['File Name'], item['File MD5']])
        print(f"OneDrive index data written to {file_path}")
    except Exception as e:
        print(f"Error writing OneDrive index file: {e}")

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
            print(f"Authenticated with Microsoft Graph API")
            return token_response['access_token']
        else:
            print(f"Token response did not contain access token: {token_response}")
            return None
    except Exception as e:
        print(f"Exception during authentication: {e}")
        return None

def calculate_md5(file_content):
    md5_hash = hashlib.md5()
    md5_hash.update(file_content)
    return md5_hash.hexdigest()

def create_db_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to SQLite database {db_file}")
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
    return conn

def create_table(conn):
    try:
        sql_create_hashes_table = """CREATE TABLE IF NOT EXISTS hashes (
                                        id integer PRIMARY KEY,
                                        item_id text NOT NULL,
                                        quickxorhash text NOT NULL,
                                        md5hash text NOT NULL
                                    );"""
        cursor = conn.cursor()
        cursor.execute(sql_create_hashes_table)
        print(f"Table 'hashes' ensured in SQLite database")
    except sqlite3.Error as e:
        print(f"Error creating table in SQLite database: {e}")

def insert_hash(conn, item_id, quickxorhash, md5hash):
    sql = '''INSERT INTO hashes(item_id, quickxorhash, md5hash) VALUES(?,?,?)'''
    cursor = conn.cursor()
    cursor.execute(sql, (item_id, quickxorhash, md5hash))
    conn.commit()
    print(f"Inserted hash for item ID {item_id} into database")

def update_hash(conn, item_id, quickxorhash, md5hash):
    sql = '''UPDATE hashes SET quickxorhash = ?, md5hash = ? WHERE item_id = ?'''
    cursor = conn.cursor()
    cursor.execute(sql, (quickxorhash, md5hash, item_id))
    conn.commit()
    print(f"Updated hash for item ID {item_id} in database")

def get_hash(conn, item_id):
    sql = '''SELECT quickxorhash, md5hash FROM hashes WHERE item_id = ?'''
    cursor = conn.cursor()
    cursor.execute(sql, (item_id,))
    return cursor.fetchone()

def get_onedrive_users_drive_items(graph_access_token, tenant_id, conn, scan_all_users, scan_users):
    headers = {
        'Authorization': 'Bearer ' + graph_access_token,
        'Content-Type': 'application/json'
    }
    users_endpoint = "https://graph.microsoft.com/v1.0/users"
    users_response = requests.get(users_endpoint, headers=headers).json()
    
    object_details = []
    
    for user in users_response.get('value', []):
        if scan_all_users or user['userPrincipalName'] in scan_users:
            print(f"{tenant_id}\\{user['userPrincipalName']}")
            drive_endpoint = f"https://graph.microsoft.com/v1.0/users/{user['id']}/drive/root/children"
            drive_response = requests.get(drive_endpoint, headers=headers).json()
            
            for item in drive_response.get('value', []):
                if 'file' in item:
                    print(f"Processing file: {item['name']}")
                    item_details = {
                        'User': user['userPrincipalName'],
                        'File Name': item['name'],
                        'File Hash': item.get('file', {}).get('hashes', {}).get('quickXorHash', ''),
                        'Item Id': item['id'],
                        'Drive Id': item['parentReference']['driveId']
                    }

                    quickXorHash = item_details['File Hash']
                    existing_hash = get_hash(conn, item_details['Item Id'])
                    
                    if existing_hash:
                        stored_quickXorHash, stored_md5hash = existing_hash
                        if stored_quickXorHash == quickXorHash:
                            item_details['File MD5'] = stored_md5hash
                            print(f"Existing hash matches for {item_details['File Name']}, using stored MD5")
                        else:
                            print(f"Hash mismatch for {item_details['File Name']}, recalculating MD5")
                            file_url = f"https://graph.microsoft.com/v1.0/drives/{item_details['Drive Id']}/items/{item_details['Item Id']}/content"
                            file_response = requests.get(file_url, headers=headers)
                            file_content = file_response.content
                            md5_hash = calculate_md5(file_content)
                            update_hash(conn, item_details['Item Id'], quickXorHash, md5_hash)
                            item_details['File MD5'] = md5_hash
                            print(f"Updated MD5: {md5_hash}")
                    else:
                        print(f"No hash found for {item_details['File Name']}, calculating MD5")
                        file_url = f"https://graph.microsoft.com/v1.0/drives/{item_details['Drive Id']}/items/{item_details['Item Id']}/content"
                        file_response = requests.get(file_url, headers=headers)
                        file_content = file_response.content
                        md5_hash = calculate_md5(file_content)
                        insert_hash(conn, item_details['Item Id'], quickXorHash, md5_hash)
                        item_details['File MD5'] = md5_hash
                        print(f"New MD5: {md5_hash}")

                    object_details.append(item_details)

    return object_details

def write_onedrive_drive_items(data, file_path):
    try:
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["User Email", "File Name", "MD5"])
            for item in data:
                writer.writerow([item['User'], item['File Name'], item['File MD5']])
        print(f"OneDrive index data written to {file_path}")
    except Exception as e:
        print(f"Error writing OneDrive index file: {e}")

def process_accounts(config):
    all_objects = []
    output = config.get('storage', {})
    s3_output = output.get('AWS', {})
    retain_files = s3_output.get('retainFiles', False)
    if s3_output:
        s3_output_client = connect_s3(s3_output['accessKey'], s3_output['secretAccessKey'])
    
    aws_inputs = config.get('inputs', {}).get('AWS', {}).get('awsAccounts', [])
    for account in aws_inputs:
        s3 = connect_s3(account['accessKey'], account['secretAccessKey'])
        if s3:
            buckets = list_s3_buckets(s3, account['scanAllBuckets'], account.get('buckets', []), account['accessKey'])
            objects = list_s3_objects(s3, buckets)
            all_objects.extend(objects)
    
    local_aws_hash_file = None
    if s3_output and all_objects:
        local_aws_hash_file = write_s3_object_list(s3_output_client, s3_output['bucketName'], s3_output['awsHashIndexFile'], all_objects, retain_files)
        if not retain_files and local_aws_hash_file:
            os.remove(local_aws_hash_file)
            print(f"Local AWS hash index file {local_aws_hash_file} removed")

    microsoft_inputs = config.get('inputs', {}).get('Microsoft', {}).get('onedriveAccounts', [])
    db_file = s3_output.get('microsoftMetadataDB')
    onedrive_index_file = s3_output.get('onedriveIndexFile')
    if s3_output and db_file and onedrive_index_file:
        local_db_file = db_file.split('/')[-1]
        print(f"Checking for database file {db_file} in S3 bucket {s3_output['bucketName']}")
        db_exists = download_db_file(s3_output_client, s3_output['bucketName'], db_file, local_db_file)
        conn = create_db_connection(local_db_file)
        if conn is not None:
            if not db_exists:
                create_table(conn)
            all_onedrive_items = []
            for onedrive_account in microsoft_inputs:
                graph_token = authenticate_onedrive(onedrive_account)
                if graph_token:
                    items = get_onedrive_users_drive_items(graph_token, onedrive_account['tenant_id'], conn, onedrive_account.get('scanAllUsers', False), onedrive_account.get('scanUsers', []))
                    all_onedrive_items.extend(items)
                else:
                    print(f"Failed to authenticate with Microsoft Graph API for OneDrive account: {onedrive_account}")
            write_onedrive_drive_items(all_onedrive_items, onedrive_index_file)
            conn.close()
            print(f"Uploading database file {local_db_file} to S3 bucket {s3_output['bucketName']}")
            upload_db_file(s3_output_client, s3_output['bucketName'], db_file, local_db_file)
            os.remove(local_db_file)
            print(f"Local database file {local_db_file} removed")
            if not retain_files:
                os.remove(onedrive_index_file)
                print(f"Local OneDrive index file {onedrive_index_file} removed")

if __name__ == "__main__":
    config_path = 'config.yaml'
    config = load_config(config_path)
    print("Starting account processing")
    process_accounts(config)
    print("Account processing completed")
