import boto3
import pandas as pd
import io

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    body = response['Body'].read().decode('utf-8')
    
    df = pd.read_csv(io.StringIO(body))
    # TODO: Preprocess, add gain/loss, clean rows, etc.
    
    # Save to new bucket
    processed_csv = df.to_csv(index=False)
    s3.put_object(
        Bucket='your-processed-bucket-name',
        Key=raw_key.replace("uploads/", "processed/"),
        Body=processed_csv.encode('utf-8')
    )
    
    return {"statusCode": 200, "body": "File processed successfully."}
