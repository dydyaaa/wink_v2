import boto3
from fastapi import UploadFile
from src.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME, S3_ENDPOINT_URL
import tempfile
import json

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT_URL
)

async def upload_file_to_s3(file: UploadFile, key: str) -> str:
    content = await file.read()
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content)
    return f"s3://{S3_BUCKET_NAME}/{key}"

def download_file_from_s3(key: str, local_path: str):
    s3_client.download_file(S3_BUCKET_NAME, key, local_path)

def upload_json_to_s3(data: dict, key: str):
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body)
