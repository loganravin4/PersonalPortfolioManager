import boto3
import pandas as pd
import pymysql
import io
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # get uploaded file from S3
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    raw_key = event['Records'][0]['s3']['object']['key']
    response = s3.get_object(Bucket=raw_bucket, Key=raw_key)
    body = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))

    # clean and preprocess
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df[['Stock', 'Returns_in_portfolio', 'Weights']]
    df['Weighted Return'] = df['Returns_in_portfolio'] * df['Weights']
    total_return = df['Weighted Return'].sum()
    df['Total Portfolio Return'] = total_return
    df = df.where(pd.notnull(df), None)

    # connect to RDS
    connection = pymysql.connect(
        host=os.environ['RDS_HOST'],
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD'],
        database=os.environ['DATABASE'],
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

        # get the updated rows
        with connection.cursor() as cursor:
            cursor.execute("SELECT Stock, Returns_in_portfolio, Weights, `Weighted Return`, `Total Portfolio Return` FROM holdings;")
            rows = cursor.fetchall()

    finally:
        connection.close()

    # build dataframe manually
    holdings_df = pd.DataFrame(rows)

    # save the updated holdings to S3
    holdings_csv = holdings_df.to_csv(index=False)
    s3.put_object(
        Bucket=os.environ['PROCESSED_BUCKET'],
        Key="processed/latest_holdings.csv",
        Body=holdings_csv.encode('utf-8')
    )

    return {
        "statusCode": 200,
        "body": "File processed, RDS updated, and latest holdings saved to S3 correctly."
    }