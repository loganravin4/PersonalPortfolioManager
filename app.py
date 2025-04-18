import streamlit as st
import boto3
import uuid
import os
from dotenv import load_dotenv
from graphs import show_basic_graphs
from advanced_graphs import show_stock_chart, show_portfolio_optimization

load_dotenv()

RAW_BUCKET = os.getenv("RAW_BUCKET")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

st.title("Personal Portfolio Manager")
st.write("Upload your portfolio CSV and view your analysis!")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    file_id = str(uuid.uuid4())
    s3.upload_fileobj(
        uploaded_file, RAW_BUCKET, f"uploads/{file_id}.csv"
    )
    st.success("File uploaded successfully! Processing... Please wait a few seconds then refresh 🔄")

if st.button("Load Latest Portfolio Visualizations"):
    st.header("Basic Portfolio Visualizations")
    show_basic_graphs()

    st.header("Advanced Stock Insights")
    ticker = st.text_input("Enter a stock ticker to view candlestick chart (e.g., AAPL):")
    if ticker:
        show_stock_chart(ticker.strip().upper())

    st.header("Optimized Portfolio Allocation")
    user_data = show_basic_graphs(return_data=True)
    if user_data is not None:
        show_portfolio_optimization(user_data['Stock'].tolist())