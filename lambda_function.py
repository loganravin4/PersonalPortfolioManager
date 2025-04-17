import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # get the uploaded file
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    response = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    body = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df[['Stock', 'Returns_in_portfolio', 'Weights']]

    # calculate weighted return
    df['Weighted Return'] = df['Returns_in_portfolio'] * df['Weights']

    # calculate total portfolio return
    total_return = df['Weighted Return'].sum()

    # add "Return"
    df['Total Portfolio Return'] = total_return
    df = df.fillna(0)

    connection = pymysql.connect(
        host=os.getenv['RDS_HOST'],
        user=os.getenv['RDS_USER'],
        password=os.getenv['RDS_PASSWORD'],
        database=os.getenv['DATABASE'],
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            for index, row in df.iterrows():
                sql = """
                INSERT INTO holdings (Stock, Returns_in_portfolio, Weights, `Weighted Return`, `Total Portfolio Return`)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    Returns_in_portfolio = VALUES(Returns_in_portfolio),
                    Weights = VALUES(Weights),
                    `Weighted Return` = VALUES(`Weighted Return`),
                    `Total Portfolio Return` = VALUES(`Total Portfolio Return`);
                """
                cursor.execute(sql, (
                    row['Stock'],
                    row['Returns_in_portfolio'],
                    row['Weights'],
                    row['Weighted Return'],
                    row['Total Portfolio Return']
                ))
        connection.commit()
    finally:
        connection.close()

    # save processed file back to S3
    processed_csv = df.to_csv(index=False)
    s3.put_object(
        Bucket=os.getenv['PROCESSED_BUCKET'],
        Key=raw_key.replace("uploads/", "processed/"),
        Body=processed_csv.encode('utf-8')
    )

    return {"statusCode": 200, "body": "File processed and saved successfully."}