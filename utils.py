import boto3
import pandas as pd
from io import StringIO
import streamlit as st
import re

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

def check_personal_information(query):
    # Regular expression for a basic email pattern
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    # Regular expression for a basic mobile phone number pattern (this will vary greatly by country)
    # This is a simplistic pattern; you might need to adjust it to the specific format you expect
    phone_pattern = r'\b(\+?\d{1,3}[\s-]?)?\(?\d{2,4}\)?[\s-]?\d{3,4}[\s-]?\d{3,4}\b'


    if re.search(email_pattern, query) or re.search(phone_pattern, query):
        return False
    else:
        return True



def check_query(query):
    if check_personal_information(query) is False:
        return False
    else:
        return True


# Example usage
example_string = 'This is a test string with an email example@example.com in it.'
print(check_query(example_string)) # Should print False

example_string2 = 'This is a clean string without any emails or phone numbers.'
print(check_query(example_string2)) # Should print True