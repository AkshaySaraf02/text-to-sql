import streamlit as st
import pdb
from cosine_similarity import cosine_similarity_score
import PyPDF2 as pdf
from openai import OpenAI
from config import OPEN_AI_API_KEY


client = OpenAI(api_key=OPEN_AI_API_KEY)

def text_extraction(file):
    text = ""
    if file.name.split(".")[-1] == "pdf":
        # pdb.set_trace()
        raw_text = pdf.PdfReader(file)
        for page in range(len(raw_text.pages)):
            page = raw_text.pages[page]
            text += str(page.extract_text())
        return text
    else:
        for line in file:
            text += str(line).replace("\\r\\n", " ").replace("\\t", "   ").replace("b'", "").replace("'", '')
        return text


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
        required_dbs = []   
        dbs = eval(db_schema)
        for db in dbs:
            print(db["table_name"] + f" {cosine_similarity_score(query, str(db))}")
            if cosine_similarity_score(prompt_text=query , context_text=str(db)) > 0:
                required_dbs.append(db)

        required_db_table_names = [d.get(list(d.keys())[0]) for d in required_dbs]

        st.subheader("Needed tables using Cosine Similarity")
        for i, db in enumerate(required_dbs):
            st.write(i+1,". ", db["table_name"])

        relevant_tables = [db for db in dbs if db["table_name"] in required_db_table_names]
        formatted_tables = ",\n".join([str(table) for table in relevant_tables])


        completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "system", "content": f"""
    
        You are SQL Expert, based on the database schema info and KPI information from user generate a sql query.
        Understand the context given for each schema and KPI before calculating to better understand the requirement.
        It is not necessary to use all the tables, only use the ones you think are required. Any column you dont recognize you must for sure understand from KPI info given below and calculate based on that only.
        Don't give any explanation unless asked, just output the query. Ask if theres any doubt, don't output wrong info. 
        
        Note: Below given information/context is in the form of list of dictionaries.

                Database schema: [{formatted_tables}],
                KPIs data: {kpis}

            """},
    {"role": "user", "content": f"{query}"}])
        

        with st.expander("See Complete Query"):
            st.subheader("Original Final Prompt:")
            st.json({
                "user_query": query,
                "system_prompt": f"""
                    You are an SQL Expert. Based on the identified relevant tables from the database schema and KPI information from the user, generate a SQL query.
                    Understand the context given for each table before calculating to better understand the requirement.
                    It is not necessary to use all the tables; only use the ones identified through cosine similarity.

                    Note: Below given information/context is in the form of a list of dictionaries.

                    Database schema: [{formatted_tables}],
                    KPIs data: {kpis}
                """,
                "database_schema": relevant_tables,
                "kpis_data": kpis
            })


        st.subheader("Query: ")
        st.code(completion.choices[0].message.content, language="sql")