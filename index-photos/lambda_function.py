import boto3
import json
import datetime
import requests
from requests_aws4auth import AWS4Auth

# ------------------------
# Configuration
# ------------------------
region = 'us-east-1'
es_host = 'search-photos-xyfnvddh2ulus663bd4vesnvju.us-east-1.es.amazonaws.com'

# AWS clients
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

# ------------------------
# Lambda handler
# ------------------------
def lambda_handler(event, context):
    print("Lambda triggered!")
    print("Received event:", json.dumps(event))

    # 🔑 Get fresh temporary AWS credentials for this invocation
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    
    awsauth = AWS4Auth(
        creds.access_key,       # positional
        creds.secret_key,       # positional
        region,                 # positional
        'es',                   # positional
        session_token=creds.token  # keyword
    )


    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(f"Processing file: {key} from bucket: {bucket}")

        # 1️⃣ Detect Rekognition labels
        try:
            response = rekognition.detect_labels(
                Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                MaxLabels=10
            )
            labels = [label['Name'] for label in response['Labels']]
            print("Detected Rekognition labels:", labels)
        except Exception as e:
            print("Error detecting labels:", e)
            labels = []

        # 2️⃣ Get custom labels from S3 metadata
        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            custom_labels = obj['Metadata'].get('customlabels', '')
            if custom_labels:
                labels.extend([label.strip() for label in custom_labels.split(',')])
            print("Custom labels:", custom_labels)
        except Exception as e:
            print("Error reading custom labels:", e)

        # 3️⃣ Build JSON document for OpenSearch
        doc = {
            "objectKey": key,
            "bucket": bucket,
            "createdTimestamp": datetime.datetime.utcnow().isoformat(),
            "labels": labels
        }
        print("Document to index:", doc)

        # 4️⃣ Index into OpenSearch using requests + SigV4 auth
        try:
            url = f"https://{es_host}/photos/_doc"
            response = requests.post(url, auth=awsauth, json=doc)
            print("OpenSearch response:", response.status_code, response.text)
        except Exception as e:
            print("Error indexing to OpenSearch:", e)

    return {
        'statusCode': 200,
        'body': json.dumps('Photo indexing complete!')
    }
