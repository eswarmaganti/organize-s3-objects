import boto3
from datetime import datetime
from pprint import pprint
from typing import Dict, List
import jmespath
import json


def lambda_handler(event, context):

    s3 = boto3.client("s3")
    bucket_name: str = "eswar-maganti-test-bucket"

    # mapping the directory name
    today = datetime.today()
    dir_name: str = f"{today.strftime('%Y%m%d')}"

    # Fetching S3 Objects in bucket
    s3_objects_list: Dict = s3.list_objects_v2(Bucket=bucket_name)
    s3_object_names = jmespath.search("Contents[].Key",s3_objects_list)
    print(f"*** Info: The Available Objects in S# Bucket-{bucket_name} is: {s3_object_names} ***")

    # Creating a new directory to organize s3 objects
    if dir_name not in s3_object_names:
        s3.put_object(Bucket=bucket_name, Key=f"{dir_name}/")
        print(f"*** Success: New directory {dir_name} is created successfully ***")
    else:
        print(f"*** Info: The directory {dir_name} is already created in the bucket ***")

    # organizing the s3 buckets
    for s3_object in s3_objects_list.get("Contents"):
        obj_last_modified_date = s3_object.get("LastModified")
        obj_dir_name = obj_last_modified_date.strftime('%Y%m%d')
        obj_name = s3_object.get("Key")

        # move s3 objects to a new directory
        if obj_dir_name == dir_name and '/' not in obj_name:
            s3.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/{obj_name}', Key=f'{dir_name}/{obj_name}')
            print(f"*** Info: {obj_name} is moved to {dir_name}/{obj_name} successfully ***")
            s3.delete_object(Bucket=bucket_name, Key=obj_name)
            print(f"*** Info: {obj_name} is successfully purged ***")

        # if the direcotry already exists and object is not organized
        if obj_dir_name != dir_name and '/' not in obj_name and f"{obj_dir_name}/" in s3_object_names:
            s3.copy_object(Bucket=bucket_name, CopySource=f'{bucket_name}/{obj_name}', Key=f'{obj_dir_name}/{obj_name}')
            print(f"*** Info: {obj_name} is moved to {obj_dir_name}/{obj_name} successfully ***")
            s3.delete_object(Bucket=bucket_name, Key=obj_name)
            print(f"*** Info: {obj_name} is successfully purged ***")
    else:
        print(f"*** Success: The organizing of S3 Bucket: {bucket_name} is successfully completed ***")

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully organized the S3 Bucket {bucket_name}')
    }


# lambda_handler(1,2)