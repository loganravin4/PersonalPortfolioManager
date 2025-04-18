import pandas as pd
import numpy as np
import boto3
import io
import os
import yfinance as yf
import mplfinance as mpf
from pypfopt import EfficientFrontier, risk_models, expected_returns
from dotenv import load_dotenv
import matplotlib.pyplot as plt
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

def show_portfolio_optimization(user_stocks):
    user_stocks = pd.Series(user_stocks).dropna().tolist()
    user_stocks = [t.strip().upper() for t in user_stocks]

    try:
        data = yf.download(user_stocks, period="1y", threads=False, progress=False)["Close"]
    except Exception as e:
        st.error(f"Error fetching stock data: {e}")
        return

    if data.empty:
        st.error("Failed to download stock data. Please check tickers or try again later.")
        return

    try:
        returns = expected_returns.mean_historical_return(data)
        cov_matrix = risk_models.sample_cov(data)

        if returns.isnull().all():
            st.error("No valid return data available for the selected stocks.")
            return

        ef = EfficientFrontier(returns, cov_matrix)
        weights = ef.max_sharpe()
        cleaned_weights = ef.clean_weights()

        st.subheader("Optimized Portfolio Weights")
        st.write(cleaned_weights)

        weights_df = pd.DataFrame.from_dict(cleaned_weights, orient='index', columns=['Weight'])

        fig, ax = plt.subplots()
        ax.bar(weights_df.index, weights_df['Weight'])
        ax.set_ylabel("Weight")
        ax.set_xlabel("Stock")
        ax.set_title("Optimized Portfolio Weights")
        st.pyplot(fig)

    except Exception as e:
        st.error(f"Portfolio optimization failed: {e}")