import boto3
import pandas as pd
from io import StringIO
import streamlit as st

def s3_export(df, file_name):
    session = boto3.client('s3',
    aws_access_key_id = st.secrets.s3.aws_access_key_id,
    aws_secret_access_key = st.secrets.s3.aws_secret_access_key)

    csv_buffer=StringIO()
    df.to_csv(csv_buffer,index=False)

    key = file_name
    bucket='reon.etl-data.sg'
    response = session.put_object(Body=csv_buffer.getvalue(),Bucket=bucket,Key=key)
    print(response)
    # print(f'File uploaded to {bucket}.')
    return response



def s3_import(file_name):

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=st.secrets.s3.aws_access_key_id,
        aws_secret_access_key=st.secrets.s3.aws_secret_access_key
    )


    obj = s3.get_object(Bucket='reon.etl-data.sg', Key=file_name)

    # Read the file content to a pandas DataFrame
    data = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    if data.shape[0] > 0:
        print("Training Data imported Sucessfully")
    return data
