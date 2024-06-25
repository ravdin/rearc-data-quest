import json
from bs4 import BeautifulSoup
import hashlib
import requests
import boto3
import os

base_url = 'https://download.bls.gov'
file_path = '/pub/time.series/pr/'
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
s3_file_path = os.environ['S3_FILE_PATH']
bucket_name = os.environ['BUCKET_NAME']

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        downloaded_files = set()
        # Download each file that's found from the link.
        # Compare the SHA256 hash to the existing file in the S3 bucket.
        # If the file doesn't exist in s3 or the hash doesn't match, upload to s3.
        for file_name, file_link in list_data_files():
            sha256_hash = hashlib.sha256()
            content = fetch_data_file(file_link)
            if content is not None:
                downloaded_files.add(f'{s3_file_path}/{file_name}')
                content_bytes = bytes(content, encoding='utf-8')
                sha256_hash.update(content_bytes)
                checksum = sha256_hash.hexdigest()

                if read_s3_object_checksum(file_name) != checksum:
                    s3.put_object(
                        Body=content_bytes,
                        Bucket=bucket_name,
                        Key=f'{s3_file_path}/{file_name}',
                        Metadata={'checksum': checksum}
                    )
        remove_missing_files(downloaded_files)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed to upload file: {str(e)}')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('File upload successful')
    }

def read_s3_object_checksum(file_name):
    try:
        existing_object = s3.get_object(
            Bucket=bucket_name,
            Key=f'{s3_file_path}/{file_name}'
        )
    except:
        return None
    return existing_object['Metadata']['checksum']

# Compare the files in the s3 bucket to the files that have been found from the source.
# Any files in the s3 bucket that are not in the source are deleted.
def remove_missing_files(downloaded_files):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=s3_file_path
    )
    for s3_object in response['Contents']:
        if s3_object['Key'] not in downloaded_files:
            s3.delete_object(
                Bucket=bucket_name,
                Key=s3_object['Key']
            )

# Parse the HTML to determine the available data files.
# Return an iterable with a file name and link for each file.
def list_data_files():
    try:
        response = requests.get(base_url + file_path, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.HTTPError as e:
        print(f'Failed to read data directory: {str(e)}')
        return

    for link in soup.find_all('a'):
        href = link.get('href')
        if href.startswith(file_path):
            yield link.text, href

def fetch_data_file(file_url):
    try:
        response = requests.get(base_url + file_url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as e:
        print(f'Failed to read data file: {str(e)}')
        return None
