import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # get the uploaded file
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    response = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    body = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))

    # preprocess by adding total return
    df['Weighted Return'] = df['Weight'] * df['Return']
    total_return = df['Weighted Return'].sum()
    df['Total Portfolio Return'] = total_return

    # insert preprocessed data into RDS
    conn = psycopg2.connect(
        host=os.getenv('RDS_HOST'),
        database=os.getenv('DATABASE'),
        user=os.getenv('RDS_USER'),
        password=os.getenv('RDS_PASSWORD')
    )
    cur = conn.cursor()

    for index, row in df.iterrows():
        cur.execute(
            "INSERT INTO holdings (ticker, weight, return_value, weighted_return, total_portfolio_return) VALUES (%s, %s, %s, %s, %s)",
            (row['Ticker'], row['Weight'], row['Return'], row['Weighted Return'], row['Total Portfolio Return'])
        )
    conn.commit()
    cur.close()
    conn.close()

    # save processed file back to S3
    processed_csv = df.to_csv(index=False)
    s3.put_object(
        Bucket=os.getenv("PROCESSED_BUCKET"),
        Key=raw_key.replace("uploads/", "processed/"),
        Body=processed_csv.encode('utf-8')
    )

    return {"statusCode": 200, "body": "File processed and saved successfully."}