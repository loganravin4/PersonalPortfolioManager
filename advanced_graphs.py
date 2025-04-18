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

# Load processed CSV from S3
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

def show_stock_chart(ticker):
    stock_data = yf.download(ticker, start="2024-04-16", end="2025-04-16", group_by="ticker")

    if isinstance(stock_data.columns, pd.MultiIndex):
        stock_data.columns = stock_data.columns.droplevel(0)

    stock_data = stock_data.dropna(subset=["Open", "High", "Low", "Close"])

    mpf.plot(stock_data, type="candle", volume=False, show_nontrading=True, title=f"{ticker} Stock Price", style="yahoo")
    st.pyplot(plt)

def show_portfolio_optimization(user_stocks):
    user_stocks = pd.Series(user_stocks).dropna().tolist()
    user_stocks = [t.strip().upper() for t in user_stocks]

    data = yf.download(user_stocks, period="1y")["Close"]

    returns = expected_returns.mean_historical_return(data)
    cov_matrix = risk_models.sample_cov(data)

    ef = EfficientFrontier(returns, cov_matrix)
    weights = ef.max_sharpe()
    cleaned_weights = ef.clean_weights()

    weights_df = pd.DataFrame.from_dict(cleaned_weights, orient='index', columns=['Weight'])
    st.subheader("Optimized Portfolio Weights Table")
    st.dataframe(weights_df)


    st.subheader("Optimized Portfolio Weights")
    st.write(cleaned_weights)

    fig, ax = plt.subplots()
    ax.bar(cleaned_weights.keys(), cleaned_weights.values())
    ax.set_ylabel("Weight")
    ax.set_xlabel("Stock")
    ax.set_title("Optimized Portfolio Weights")
    st.pyplot(fig)