import pandas as pd
import matplotlib.pyplot as plt
import boto3
import io
import os
from dotenv import load_dotenv
import streamlit as st

load_dotenv()

def load_user_data():
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                      region_name=os.getenv('AWS_REGION'))
    bucket = os.getenv('PROCESSED_BUCKET')
    object_key = 'processed/latest_holdings.csv'

    response = s3.get_object(Bucket=bucket, Key=object_key)
    content = response['Body'].read().decode('utf-8')
    user_data = pd.read_csv(io.StringIO(content))
    return user_data

def show_basic_graphs(return_data=False):
    user_data = load_user_data()

    if user_data.empty:
        st.error("No processed portfolio data found yet.")
        return None

    fig, ax = plt.subplots()
    ax.bar(user_data['Stock'], user_data['Weights'])
    ax.set_ylabel("Weight")
    ax.set_title("Portfolio Weights")
    st.pyplot(fig)

    fig, ax = plt.subplots()
    ax.bar(user_data['Stock'], user_data['Weighted Return'])
    ax.set_ylabel("Weighted Return")
    ax.set_title("Weighted Return per Stock")
    st.pyplot(fig)

    if return_data:
        return user_data