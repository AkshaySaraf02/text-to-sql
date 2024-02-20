import streamlit as st
import pandas as pd
from cosine_similarity import cosine_similarity_scores
import PyPDF2 as pdf
from openai import OpenAI
from sql_to_data import execution_context_creation, sql_execution, data_retrieval, destroy_execution_context
import time
from sql_doctor import curated_sql
from utils import s3_import, s3_export

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
        /summaryends (This separation is very mandatory. Never ever forget to put this separation.)
        One line statement of what data and how you extracted the data purely based on the SQL query you made in a very very short and concise manner. Make sure to match what you say and what you have done in the query, both should match. also point out if you feel there is a important table missing. 
        

            """},
    {"role": "user", "content": f"{query}"}])

    sql = (completion.choices[0].message.content).split("/summaryends")[0].replace("`", "").replace("sql", "")
    interpretation = completion.choices[0].message.content.split("/summaryends")[1]

    st.session_state.sql = sql
    st.session_state.query = query

    return sql, interpretation


def modify_sql(interpretation, sql_query, feedback, formatted_tables, needed_kpis):
    completion = client.chat.completions.create(
    model = gpt_engine,
    messages=[{"role": "system", "content": f"""
    
        You are a SQL expert, You have previously generated a sql query based on a prompt from the user which you will now have to 
        modify and tailor a bit based on the user's feedback. You will use your great expertise in modifying and tailoring that existing query 
        (which was generated by you previously) based on the users given feedback or changes. 
        You'll take in to account three things: 
            1. Your prior understanding of the pre existing query (Generated by you). 
            2. The pre existing query (Generated by you).
            3. User's feeback on the query i.e the new changes user wants.

        Other inputs like database tables and kpis are also given below that may prove helpful in calculating the query.        
        Note: Below given information/context is in the form of list of dictionaries.

                Database schema: [{formatted_tables}],
                KPIs data: {needed_kpis}
        
        
        Output format:
        SQL Query
        /summaryends (This separation is very mandatory. Never ever forget to put this separation.)
        One line statement of what data and how you extracted the data purely based on the SQL query you made in a very very short and concise manner. Make sure to match what you say and what you have done in the query, both should match. also point out if you feel there is a important table missing. 
        

            """},
    {"role": "user", "content": f"""
                                    1. Interpretation: {interpretation}
                                    2. Previous query: {sql_query}
                                    3. Feedback / Changes Needed : {feedback}
                                """}])
    print(completion.choices[0].message.content)
                                    

# Initializing OpenAI Client
OPEN_AI_API_KEY = st.text_input('Enter your OpenAI API KEY')

if OPEN_AI_API_KEY[-10:] == "5uPMacTtmb":
    gpt_engine = "gpt-4-turbo-preview"
else:
    gpt_engine = "gpt-3.5-turbo"

client = OpenAI(api_key=OPEN_AI_API_KEY)


# Session state code for nested buttons. 
if "sql" and "query" not in st.session_state:
    st.session_state.sql = ""
    st.session_state.query = ""


st.title("Text to SQL 🤖")

# DB Schema & KPI Documents uploader
col1, col2 = st.columns(2, gap="medium")
with col1:
    db_schema = st.file_uploader(label="Database Schema", type=["pdf", "txt"])
with col2:
    kpis = st.file_uploader(label="KPIs", type=["pdf", "txt"])

# User's input
query = st.text_input(label="Enter your prompt")

# Suggesting sample codes using Cosine Similarity.
sample_codes = eval(str(open("sample_codes.txt", "r").read()))
suggestions = []

if len(query) > 0: 
    cosine_similarity_scores(query, sample_codes, threshold=10, output_list=suggestions, matching_key="context", name_key="name")

if len(suggestions) > 0:
    part1, part2 = st.columns(2, gap="small")
    with part1: 
        st.subheader("Do you mean the following?")
    with part2:
        for sugg in suggestions:
            st.button(sugg["name"])

# Submit button to generate the query.
submit = st.button("Submit")

if db_schema and kpis and query != "":
    db_schema = text_extraction(db_schema)
    kpis = text_extraction(kpis)
    if submit:
        dbs = eval(db_schema)
        kpis = eval(kpis)
        
        required_dbs = []  # Cosine similarity to get relevant tables from DB Schema doc.  
        cosine_similarity_scores(query, dbs, 3, label="--> DB Schema", name_key="table_name", output_list=required_dbs)

        needed_kpis = [] # Cosine similarity to get relevant KPIs from KPIs doc.
        cosine_similarity_scores(query, kpis, 5, label="--> Required KPIs", matching_key="kpi_name", name_key="kpi_name", output_list=needed_kpis)

        for kpi in needed_kpis: # Cosine similarity to get tables required to calculate selected KPIs .
            cosine_similarity_scores(kpi, dbs, 14, required_dbs, name_key="table_name", label="--> Tables from KPIs")

        required_db_table_names = [d.get(list(d.keys())[0]) for d in required_dbs]

        st.subheader("Needed tables using Cosine Similarity")
        for i, db in enumerate(required_dbs):
            st.write(i+1,". ", db["table_name"])

        relevant_tables = [db for db in dbs if db["table_name"] in required_db_table_names]
        formatted_tables = ",\n".join([str(table) for table in relevant_tables])

        # modify_sql(interpretation="Calculated sales grouped by their slab_name",
        #             sql_query="Select user_id, sum(bill_amount) from bill_summary bs join users u on u.user__id = bs.dim_event_user_id group by 1",
        #             feedback="Sorry I meant by month and not slab wise.",
        #             formatted_tables=formatted_tables, needed_kpis=needed_kpis)
        #       
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
        # print(sql)
        sql_query = curated_sql(sql)
        # print("Curated SQL: \n", sql_query)
    
        # sql_query = """ 
        #                 SELECT      
        #                 d.month,      
        #                 d.year,      
        #                 round(SUM(bs.bill_amount) / COUNT(DISTINCT bs.bill_id),0) AS Average_Transaction_Value 
        #                 FROM      
        #                 read_api_150877.users u 
        #                 JOIN      read_api_150877.bill_summary bs ON u.user_id = bs.dim_event_user_id 
        #                 JOIN      read_api_150877.date d ON bs.dim_event_date_id = d.date_id 
        #                 WHERE      u.slab_name = 'PLATINUM'      
        #                 AND d.date >= '2023-04-01' 
        #                 GROUP BY      d.year, d.month 
        #                 ORDER BY      d.year, d.month
        #             """
        # sql_query = "SELECT * FROM read_api_150877.zone_tills LIMIT 10"
        
        # sql_query = "use read_api_150877; SELECT * FROM zone_tills LIMIT 10;"
        
        #   SQL to Data Generation
        
        try:
            cluster_id= st.secrets.databricks.cluster_id
            context_id=execution_context_creation()
            command_id=sql_execution(sql_query, context_id, cluster_id)
            print("Waiting for result retrieval...")
            for i in range(30,0,-1):
                print("{:2d}".format(i), end="\r", flush=True)
                time.sleep(1)
            df=data_retrieval(context_id, command_id,cluster_id)
            if df.shape[0]>0:
                print("Result Retrieved Successfully  proceeding with destruction of execution context")
            else:
                print("Still waiting for command to execute, please try after sometime, proceeding with destruction of execution context")
            
            destroy_execution_context(cluster_id, context_id)
            st.dataframe(df)
            
        except:
            print("Some error has occured please check the flow")
            destroy_execution_context(cluster_id, context_id)

        




# Feedback button         
if len(st.session_state.sql) > 0:
    if st.button("Mark as Correct"):
        print("Correct Query") 
        st.subheader("Thank you for your feedback 👍🏻. Try some more prompts.")
        
        # Converting prompt and sql to dataframe.
        current_data = pd.DataFrame([[st.session_state.query, st.session_state.sql.replace("\n", " ")]], columns=["Prompt", "Query"])
        print(current_data)
        file_name = "analytics/data/akhil/llm_training_data/training_data.csv"
        # Appending new dataframe to previous and saving to future training data.
        training_data = s3_import(file_name)
        # training_data = pd.read_csv("training_data.csv")
        df = pd.concat([training_data, current_data], axis=0)[["Prompt", "Query"]]
        s3_export(df, file_name)
        st.session_state.sql = ""
        st.session_state.query = ""
