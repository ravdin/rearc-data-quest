import json
import boto3
import requests
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    api_url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'

    try:
        # Fetch data from the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        json_data = response.json()

        # Convert JSON data to string
        json_str = json.dumps(json_data)

        # Get S3 bucket name and file name from environment variables
        bucket_name = os.environ['BUCKET_NAME']
        file_name = os.environ['FILE_NAME']

        # Save to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_str, ContentType='application/json')

        return {
            'statusCode': 200,
            'body': json.dumps('Data saved to S3 successfully')
        }

    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error fetching data from API: {str(e)}')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error saving data to S3: {str(e)}')
        }
