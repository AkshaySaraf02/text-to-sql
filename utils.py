import boto3
import pandas as pd
from io import StringIO
import streamlit as st
import re
import pandas as pd
from ftplib import *
import io
import csv
from datetime import datetime

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

def check_prompt_for_PI(query):
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

    phone_pattern = r'\b(\+?\d{1,3}[\s-]?)?\(?\d{2,4}\)?[\s-]?\d{3,4}[\s-]?\d{3,4}\b'


    if re.search(email_pattern, query) or re.search(phone_pattern, query):
        return False
    else:
        return True



def check_query(query):
    if check_prompt_for_PI(query) is False:
        return False
    else:
        return True
    


def check_data_for_PI(df):

    # Simplified regular expressions
    mobile_regex = r'\b(\+?\d{1,3}[\s-]?)?\(?\d{2,4}\)?[\s-]?\d{3,4}[\s-]?\d{3,4}\b'  # Adjust for your country's mobile patterns
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

    has_mobile = False
    has_email = False
    # mobile_columns = []
    # email_columns = []

    for col in df.columns:
        if df[col].dtype == 'object':  # Check only object type columns
            if any(df[col].str.contains(mobile_regex)):
                has_mobile = True
                # mobile_columns.append(col)
            if any(df[col].str.contains(email_regex)):
                has_email = True
                # email_columns.append(col)
    print("PI Information in  Data? ", has_mobile or has_email)
    return has_mobile or has_email

def f3_export(df):
    try:
        now = datetime.now()
        formatted_datetime = now.strftime("%Y_%m_%d_%H_%M_%S")

        ftp = FTP('data.capillarydata.com')
        ftp.set_pasv(True)
        ftp.login(st.secrets.ftp.username, st.secrets.ftp.password)
        ftp.cwd('/Data_Import/text_to_sql/Data_Exports')
        #converting to file like object  df can be replaced by whatever dataframe name you give for the query
        buffer=io.StringIO()
        df.to_csv(buffer,index=False)
        text = buffer.getvalue()
        bio = io.BytesIO(str.encode(text))

        #Do not remove STOR  its a keyword
        ftp.storbinary('STOR data_export_{}.csv'.format(formatted_datetime), bio)
        #This is essential to close open ports and prevent hacking into your ftp account
        ftp.quit()
        return True
    except Exception as e:
        print(f"Error during FTP Export: {e}")
        return False