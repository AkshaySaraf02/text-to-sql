import streamlit as st
import pandas as pd
from cosine_similarity import cosine_similarity_score
import PyPDF2 as pdf
from openai import OpenAI
# from config import OPEN_AI_API_KEY

OPEN_AI_API_KEY = st.text_input('Enter your OpenAI API KEY')

if "sql" and "query" not in st.session_state:
    st.session_state.sql = ""
    st.session_state.query = ""

if OPEN_AI_API_KEY[-10:] == "5uPMacTtmb":
    gpt_engine = "gpt-4-turbo-preview"
else:
    gpt_engine = "gpt-3.5-turbo"

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

def generate(formatted_tables, needed_kpis, query):
    completion = client.chat.completions.create(
    model = gpt_engine, # have to make this GPT-4
    messages=[{"role": "system", "content": f"""
    
        You are SQL Expert, based on the database schema info and KPI information from user generate a sql query.
        Understand the context given for each schema and KPI before calculating to better understand the requirement.
        It is not necessary to use all the tables, only use the ones you think are required. Any column you dont recognize you must for sure understand from KPI info given below and calculate based on that only.
        Don't give any explanation unless asked, just output the query. Ask if theres any doubt, don't output wrong info. 
        
        Note: Below given information/context is in the form of list of dictionaries.

                Database schema: [{formatted_tables}],
                KPIs data: {needed_kpis}
        
        
        Output format:
        SQL Query
        /summaryends (This separation is mandatory)
        One line statement of what data and how you extracted the data purely based on the SQL query you made in a very very short and concise manner. Make sure to match what you say and what you have done in the query, both should match. also point out if you feel there is a important table missing. 
        

            """},
    {"role": "user", "content": f"{query}"}])

    sql = (completion.choices[0].message.content).split("/summaryends")[0].replace("`", "").replace("sql", "")
    interpretation = completion.choices[0].message.content.split("/summaryends")[1]

    st.session_state.sql = sql
    st.session_state.query = query

    return sql, interpretation

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
        kpis = eval(kpis)

        print("\nDB Schema Cosines ")
        for db in dbs:
            print(db["table_name"] + f" {cosine_similarity_score(query, str(db))}")
            if cosine_similarity_score(prompt_text=query , context_text=str(db) )> 5:
                required_dbs.append(db)

        needed_kpis = []
        for kpi in kpis:
            print(kpi["kpi_name"], cosine_similarity_score(query, str(kpi["kpi_name"])))
            if cosine_similarity_score(prompt_text=query , context_text=str(kpi["kpi_name"])) > 5:
                print("Selected KPI: ", kpi["kpi_name"])
                needed_kpis.append(kpi)
   
        for kpi in needed_kpis:
            for db in dbs:
                print("KPI Cosines: ", db["table_name"], cosine_similarity_score(str(db), str(kpi)))
                if cosine_similarity_score(str(db), str(kpi)) > 14:     # Might have to fine tune this later.  
                    if db not in required_dbs: 
                        print("FROM KPI DOC: ", db["table_name"])
                        required_dbs.append(db)

        required_db_table_names = [d.get(list(d.keys())[0]) for d in required_dbs]

        st.subheader("Needed tables using Cosine Similarity")
        for i, db in enumerate(required_dbs):
            st.write(i+1,". ", db["table_name"])

        relevant_tables = [db for db in dbs if db["table_name"] in required_db_table_names]
        formatted_tables = ",\n".join([str(table) for table in relevant_tables])

        sql, interpretation = generate(formatted_tables, needed_kpis, query)
        
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

        st.subheader("LLM's Interpretation: ")
        st.write(interpretation)

        st.subheader("Query: ")
        st.code(sql, language="sql")

if len(st.session_state.sql) > 0:
    if st.button("Mark as Correct"):
        print("Correct Query") 
        st.subheader("Thank you for your feedback 👍🏻. Try some more prompts.")
        
        current_data = pd.DataFrame([[st.session_state.query, st.session_state.sql.replace("\n", " ")]], columns=["Prompt", "Query"])
        print(current_data)

        training_data = pd.read_csv("training_data.csv")
        pd.concat([training_data, current_data], axis=0)[["Prompt", "Query"]].to_csv("training_data.csv", index=False)
        st.session_state.sql = ""
        st.session_state.query = ""