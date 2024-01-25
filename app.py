import streamlit as st
import PyPDF2 as pdf
from openai import OpenAI
from config import OPEN_AI_API_KEY

client = OpenAI(api_key=OPEN_AI_API_KEY)

def text_extraction(file):
    text = ""
    if file.name.split(".")[-1] == "pdf":
        raw_text = pdf.PdfReader(file)
        for page in range(len(raw_text.pages)):
            page = raw_text.pages[page]
            text += str(page.extract_text())
        return text
    else:
        for line in file:
            text += str(line).replace("\\r\\n", " ").replace("\\t", "   ").replace("b'", "").replace("'", '')
        return text.replace("b'", "")


st.title("Text to SQL 🤖")

col1, col2 = st.columns(2, gap="medium")
with col1:
    db_schema = st.file_uploader(label="Database Schema", type=["pdf", "txt"])
with col2:
    kpis = st.file_uploader(label="KPIs", type=["pdf", "txt"])

query = st.text_input(label="Enter your prompt")
submit = st.button("Submit")


if db_schema and kpis and query != "":
    db_schema = text_extraction(db_schema)
    kpis = text_extraction(kpis)
    if submit:
        completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "system", "content": f"""
        
            You are SQL Expert, based on the database schema info and KPI information from user generate a sql query.
            Understand the context given for each schema and KPI before calculating to better understand the requirement It is not necessary to use all the tables, only use the ones you think are required. Any column you dont recognize you must for sure understand from KPI info given below and calculate based on that only.
            Don't give any explanation unless asked, just output the query. Ask if theres any doubt, don't output wrong info. 
            
            Note: Below given information/context is in the form of list of dictionaries.

                    Database schema: [{db_schema}],
                    KPIs data: {kpis}

                """},
        {"role": "user", "content": f"{query}"}])
        
        st.subheader("Query: ")
        st.code(completion.choices[0].message.content, language="sql")