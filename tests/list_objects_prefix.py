import boto3
import botocore

# Set your AWS credentials
aws_access_key = 'ak'
aws_secret_key = 'sk'
#aws_region = 'eu-south-2'  # Change to your desired region

s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
bucket_name = 'eurac-eo'
file_name = 'process.json'
prefix = 'test'

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

for page in pages:
    for obj in page['Contents']:
        print(obj['Key'])

