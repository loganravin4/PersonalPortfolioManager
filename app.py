import streamlit as st
import boto3
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

RAW_BUCKET = os.getenv("RAW_BUCKET")
s3 = boto3.client("s3")

st.title("Upload Your Portfolio")
st.write("Upload a CSV file of your holdings.")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    file_id = str(uuid.uuid4())
    s3.upload_fileobj(
        uploaded_file, RAW_BUCKET, f"uploads/{file_id}.csv"
    )
    st.success("File uploaded successfully!")